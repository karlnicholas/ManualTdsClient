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

public class TdsClientRandomPool {
  public static void main(String[] args) {
    new TdsClientRandomPool().run();
  }

  private void run() {
    ConnectionFactory baseConnectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(ConnectionFactoryOptions.DRIVER, "javatdslib")
        .option(HOST, "localhost")
        .option(PORT, 1433)
        .option(PASSWORD, "reactnonreact")
        .option(USER, "reactnonreact")
        .option(DATABASE, "reactnonreact")
        .option(TdsLibOptions.TRUST_SERVER_CERTIFICATE, true)
        .build());

    ConnectionPoolConfiguration poolConfiguration = ConnectionPoolConfiguration.builder(baseConnectionFactory)
        .initialSize(10)
        .maxSize(50)
        .maxIdleTime(Duration.ofMinutes(10))
        .maxCreateConnectionTime(Duration.ofSeconds(5))
        .build();

    ConnectionPool connectionPool = new ConnectionPool(poolConfiguration);

    System.out.println("Connecting to database pool for Async Load Testing...");

    Mono.usingWhen(
            Mono.just(connectionPool),
            this::runSql,
            pool -> pool.disposeLater().doOnSuccess(v -> System.out.println("\nConnection Pool safely closed."))
        )
        .doOnError(t -> System.err.println("Connection/Run Failed: " + t.getMessage()))
        .block();
  }

  public Mono<Void> runSql(ConnectionPool pool) {
    return fetchColumnNames(pool)
        .flatMap(allColumns -> fetchMaxId(pool)
            .flatMap(maxId -> {
              if (maxId <= 0) {
                return Mono.error(new IllegalStateException("Table is empty (maxId=0), cannot run tests."));
              }
              return executeLoadTest(pool, allColumns, maxId);
            })
        )
        // Explicitly trigger the audit after the 6000 queries complete
        .then(Mono.defer(() -> auditPool(pool)));
  }

  private Mono<Void> auditPool(ConnectionPool pool) {
    System.out.println("\n--- Starting Post-Test Pool Integrity Audit ---");
    System.out.println("Requesting 50 concurrent connections to flush out any poisoned sockets...");

    // Request exactly 50 connections to saturate the pool's maxSize
    return Flux.range(1, 50)
        .flatMap(i -> Mono.usingWhen(
            Mono.from(pool.create()),
            conn -> Flux.from(conn.createStatement("SELECT 1 AS ping").execute())
                .flatMap(result -> result.map((row, meta) -> row.get(0, Integer.class)))
                // If the socket is hung/poisoned, this 2-second timeout will catch it
                .timeout(Duration.ofSeconds(2))
                .doOnNext(val -> System.out.println("  ✓ Connection #" + i + " Healthy"))
                .doOnError(e -> System.err.println("  [!] Connection #" + i + " FAILED: " + e.getMessage()))
                .then(), // <--- THE FIX: Converts the Flux into a Mono<Void> for usingWhen
            Connection::close
        ), 50) // Force a concurrency of 50 to lease everything simultaneously
        .then()
        .doOnSuccess(v -> System.out.println("--- Pool Audit Complete: ALL CONNECTIONS ARE HEALTHY ---\n"));
  }

  private Mono<Void> executeLoadTest(ConnectionPool pool, List<String> allColumns, int maxId) {
    System.out.println("Found max id = " + maxId + ". Starting async load test...");

    return Flux.range(1, 6000)
        .flatMap(i -> {
          ThreadLocalRandom random = ThreadLocalRandom.current();

          int numColumns = random.nextInt(allColumns.size()) + 1;
          List<String> shuffledColumns = new ArrayList<>(allColumns);
          Collections.shuffle(shuffledColumns, random);
          List<String> selectedColumns = shuffledColumns.subList(0, numColumns);

          String selectList = String.join(", ", selectedColumns);

          int maxPossibleRows = Math.min(maxId, random.nextInt(5) + 1);
          int numRows = random.nextInt(maxPossibleRows) + 1;

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

          return Mono.usingWhen(
              Mono.from(pool.create()),
              conn -> executeRandomQuery(stepName, dynamicQuery, conn.createStatement(dynamicQuery).execute(), selectedColumns),
              Connection::close
          );
        }, 256)
        .then();
  }

  // --- Latch-Free Helper Methods remain unchanged ---

  private Mono<List<String>> fetchColumnNames(ConnectionPool pool) {
    String sql = """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dbo'
              AND TABLE_NAME = 'AllDataTypes'
            ORDER BY ORDINAL_POSITION;
            """;

    return Mono.usingWhen(
            Mono.from(pool.create()),
            conn -> Flux.from(conn.createStatement(sql).execute())
                .flatMap(result -> result.map((row, meta) -> row.get(0, String.class)))
                .collectList(),
            Connection::close
        )
        .filter(columns -> !columns.isEmpty())
        .switchIfEmpty(Mono.error(new RuntimeException("No columns found in dbo.AllDataTypes!")));
  }

  private Mono<Integer> fetchMaxId(ConnectionPool pool) {
    String sql = "SELECT ISNULL(MAX(id), 0) FROM dbo.AllDataTypes";

    return Mono.usingWhen(
        Mono.from(pool.create()),
        conn -> Flux.from(conn.createStatement(sql).execute())
            .flatMap(result -> result.map((row, meta) -> row.get(0, Integer.class)))
            .single(),
        Connection::close
    );
  }

  private Mono<Void> executeRandomQuery(String stepName, String query, Publisher<? extends Result> resultPublisher, List<String> columnOrder) {
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
        .doOnNext(item -> {})
        .timeout(Duration.ofSeconds(30))
        .doOnError(error -> {
          System.err.println("[" + stepName + "] CRASHED: " + error.getMessage());
          // SPRING THE TRAP: Now 'query' is available to use as the map key
          System.err.println("   => DRIVER STATE: " + org.tdslib.javatdslib.transport.TdsTransport.queryStates.get(query));
        })
        .then();
  }
}