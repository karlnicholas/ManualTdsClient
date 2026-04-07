package com.example;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.api.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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

public class TdsClientRandomAsync {
  public static void main(String[] args) {
    new TdsClientRandomAsync().run();
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

    System.out.println("Connecting to database for Async Load Testing...");

    Mono.usingWhen(
            Mono.from(connectionFactory.create()),
            this::runSql,
            conn -> Mono.from(conn.close()).doOnSuccess(v -> System.out.println("\nConnection safely closed."))
        )
        .doOnError(t -> System.err.println("Connection/Run Failed: " + t.getMessage()))
        .block();
  }

  private Mono<Void> runSql(Connection connection) {
    return fetchColumnNames(connection)
        .flatMap(allColumns -> fetchMaxId(connection)
            .flatMap(maxId -> {
              if (maxId <= 0) {
                return Mono.error(new IllegalStateException("Table is empty (maxId=0), cannot run tests."));
              }
              return executeLoadTest(connection, allColumns, maxId);
            })
        );
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