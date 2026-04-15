package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.api.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsClientMicroPoison {
  public static void main(String[] args) {
    new TdsClientMicroPoison().run();
  }

  private void run() {
    ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(ConnectionFactoryOptions.DRIVER, "javatdslib")
        .option(HOST, "localhost")
        .option(PORT, 1433)
        .option(PASSWORD, "reactnonreact")
        .option(USER, "reactnonreact")
        .option(DATABASE, "reactnonreact")
        .option(TdsLibOptions.TRUST_SERVER_CERTIFICATE, true)
        .build());

    // Configure a simple pool for the standalone client execution
    ConnectionPoolConfiguration poolConfiguration = ConnectionPoolConfiguration.builder(connectionFactory)
        .initialSize(2)
        .maxSize(10)
        .maxIdleTime(Duration.ofMinutes(10))
        .build();

    ConnectionPool pool = new ConnectionPool(poolConfiguration);

    System.out.println("Connecting to pool for Comprehensive Binding Matrix & Way Testing...");

    // Manage the Pool lifecycle and fail-fast on errors
    Mono.usingWhen(
            Mono.just(pool),
            this::runSql,
            p -> p.disposeLater().doOnSuccess(v -> System.out.println("\nTests complete. Connection pool closed."))
        )
        .doOnError(t -> System.err.println("\n❌ Test Suite Failed: " + t.getMessage()))
        .block();
  }

  /**
   * Overloaded method: Takes a ConnectionPool, borrows a single connection,
   * runs the tests, and safely releases the connection back to the pool.
   */
  public Mono<Void> runSql(ConnectionPool pool) {
    return Mono.usingWhen(
        Mono.from(pool.create()),
        this::runSql,
        Connection::close
    );
  }

  public Mono<Void> runSql(Connection connection) {
    System.out.println("Starting Cancellation-Poison Test...");

    // Query 2 will intentionally fail at the database level
    return Flux.just("SELECT 1 AS val", "SELECT 1/0 AS val", "SELECT 2 AS val")
        .flatMap(query -> {
          System.out.println(" -> Dispatching: " + query);
          return Flux.from(connection.createStatement(query).execute())
              .flatMap(result -> result.map((row, meta) -> row.get(0, Integer.class)))
              .doOnNext(val -> System.out.println(" <- Result: " + val))
              .doOnError(e -> System.err.println(" [!] Error caught: " + e.getMessage()));
        }, 3) // Concurrency = 3 forces them all into the driver's internal queue instantly
        .then()
        // Swallow the crash at the top level so we don't drop the connection via usingWhen
        .onErrorResume(e -> {
          System.err.println("--- Stream Aborted Due to Error ---");
          return Mono.empty();
        })
        // Now, fire ONE more query sequentially to prove if the queue lock is permanently stuck
        .then(Mono.defer(() -> {
          System.out.println("\n--- Testing Socket Integrity ---");
          return Flux.from(connection.createStatement("SELECT 99 AS test_val;").execute())
              .flatMap(result -> result.map((row, meta) -> row.get(0, Integer.class)))
              .doOnNext(val -> System.out.println(" <- Integrity Check Passed: " + val))
              .then();
        }));
  }

  private Mono<Void> executeLoadTest(Connection connection, List<String> allColumns, int maxId) {
    System.out.println("Found max id = " + maxId + ". Starting async load test...");

    // 4. Execute 6000 random queries asynchronously.
    // We explicitly set concurrency to 256 (Reactor's default) to document the tuning knob.
    // If your driver/server struggles, lower this number (e.g., to 32 or 64).
    return Flux.range(1, 6000)
        .flatMap(i -> {
          // Use ThreadLocalRandom for thread-safe concurrent random generation
          ThreadLocalRandom random = ThreadLocalRandom.current();

          int numColumns = random.nextInt(allColumns.size()) + 1;
          List<String> shuffledColumns = new ArrayList<>(allColumns);
          Collections.shuffle(shuffledColumns, random);
          List<String> selectedColumns = shuffledColumns.subList(0, numColumns);

          String selectList = String.join(", ", selectedColumns);

          int maxPossibleRows = Math.min(maxId, random.nextInt(5) + 1);
          int numRows = random.nextInt(maxPossibleRows) + 1;

          // CRITICAL OPTIMIZATION: Instead of building an IntStream of maxId,
          // lazily generate a distinct stream of random integers.
          String idList = random.ints(1, maxId + 1)
              .distinct()
              .limit(numRows)
              .mapToObj(String::valueOf)
              .collect(Collectors.joining(", "));

          String whereClause = idList.isEmpty() ? "WHERE 1=0" : "WHERE id IN (" + idList + ")";

          String dynamicQuery = """
                SET TEXTSIZE -1;
                SELECT %s
                FROM dbo.AllDataTypes
                %s
                ORDER BY id;
                """.formatted(selectList, whereClause);

          if (i % 1000 == 0) {
            System.out.println("Dispatched Random Query #" + i);
          }

          String stepName = "Query #" + i;

          return Mono.defer(() ->
              executeRandomQuery(stepName, connection.createStatement(dynamicQuery).execute(), selectedColumns)
          );
        }, 256)
        .then();
  }

  // --- Latch-Free Helper Methods ---

  private Mono<List<String>> fetchColumnNames(Connection connection) {
    System.out.println("Fetching column list from INFORMATION_SCHEMA...");

    String sql = """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dbo'
              AND TABLE_NAME = 'AllDataTypes'
            ORDER BY ORDINAL_POSITION;
            """;

    return Flux.from(connection.createStatement(sql).execute())
        .flatMap(result -> result.map((row, meta) -> row.get(0, String.class)))
        .collectList()
        .doOnSuccess(columns -> System.out.println("Fetched " + columns.size() + " columns."))
        .filter(columns -> !columns.isEmpty())
        .switchIfEmpty(Mono.error(new RuntimeException("No columns found in dbo.AllDataTypes!")));
  }

  private Mono<Integer> fetchMaxId(Connection connection) {
    System.out.println("Fetching max(id) from AllDataTypes...");
    String sql = "SELECT ISNULL(MAX(id), 0) FROM dbo.AllDataTypes";

    return Flux.from(connection.createStatement(sql).execute())
        .flatMap(result -> result.map((row, meta) -> row.get(0, Integer.class)))
        .single();
  }

  private Mono<Void> executeRandomQuery(String stepName, Publisher<? extends Result> resultPublisher, List<String> columnOrder) {
    return Flux.from(resultPublisher)
        .flatMap(result -> result.map((row, meta) -> {
          StringBuilder sb = new StringBuilder();
          for (int idx = 0; idx < columnOrder.size(); idx++) {
            String colName = columnOrder.get(idx);
            Object value = row.get(idx, Object.class);
            sb.append(colName)
                .append(": ")
                .append(value == null ? "NULL" : value.toString().replace("\n", "\\n"))
                .append(" | ");
          }
          return sb.toString().trim();
        }))
        // Consume the items reactively
        .doOnNext(item -> {})
        .doOnError(error -> System.err.println("[" + stepName + "] Error: " + error.getMessage()))
        .then();
  }
}