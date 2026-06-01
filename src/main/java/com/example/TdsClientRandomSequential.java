package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TdsClientRandomSequential {
  public static void main(String[] args) throws Exception {
    new TdsClientRandomSequential().run();
  }

  private void run() {
    String r2dbcUrl = "r2dbc:mssql://reactnonreact:reactnonreact@localhost:1433/reactnonreact?trustServerCertificate=true";
    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(r2dbcUrl)).initialSize(2).maxSize(50).build());

    System.out.println("Connecting to pool for Comprehensive Binding Matrix & Way Testing...");

    long startTime = System.currentTimeMillis();

    // Manage the Pool lifecycle and fail-fast on errors
    Mono.usingWhen(
            Mono.just(pool),
            this::runSql,
            p -> p.disposeLater().doOnSuccess(v -> System.out.println("\nTests complete. Connection pool closed."))
        )
        .doOnError(t -> System.err.println("\n❌ Test Suite Failed: " + t.getMessage()))
        .doFinally(signalType -> {
          long totalTime = System.currentTimeMillis() - startTime;
          System.out.println("Total execution time: " + totalTime + " ms");
        })
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

  private Mono<Void> runSql(Connection connection) {
    // 1. Fetch columns FIRST
    return fetchColumnNames(connection)
        // 2. ONLY when columns are done, fetch max ID
        .flatMap(allColumns -> fetchMaxId(connection)
            // 3. Now we have both, proceed to the loop
            .flatMap(maxId -> {
              System.out.println("Found max id = " + maxId);
              Random random = new Random();

              // 4. Execute 6000 random queries sequentially using concatMap
              return Flux.range(1, 6000)
                  .concatMap(i -> {
                    int numColumns = random.nextInt(allColumns.size()) + 1;

                    List<String> shuffledColumns = new ArrayList<>(allColumns);
                    Collections.shuffle(shuffledColumns);
                    List<String> selectedColumns = shuffledColumns.subList(0, numColumns); // Assigned once, now effectively final!

                    String selectList = String.join(", ", selectedColumns);

                    // Changed to exactly 20 rows, or maxId if the table is smaller than 20
                    int numRows = Math.min(maxId, 20);

                    List<Integer> possibleIds = IntStream.rangeClosed(1, maxId).boxed().collect(Collectors.toList());
                    Collections.shuffle(possibleIds);
                    List<Integer> selectedIds = possibleIds.subList(0, numRows);

                    String idList = selectedIds.stream()
                        .map(String::valueOf)
                        .collect(Collectors.joining(", "));

                    String whereClause = selectedIds.isEmpty() ? "WHERE 1=0" : "WHERE id IN (" + idList + ")";

                    String dynamicQuery = """
                          SET TEXTSIZE -1;
                          SELECT %s
                          FROM dbo.AllDataTypes
                          %s
                          ORDER BY id;
                          """.formatted(selectList, whereClause);

                    if (i % 1000 == 0) {
                      System.out.println(i);
                    }

                    String stepName = "Random Query #" + i + " (" + numColumns + " cols, " + numRows + " rows)";

                    // Wrap the execution in Mono.defer() to prevent synchronous evaluation
                    return Mono.defer(() ->
                        executeRandomQuery(stepName, connection.createStatement(dynamicQuery).execute(), selectedColumns)
                    );
                  })
                  .then(); // Convert the Flux<Void> of 6000 queries into a single Mono<Void>
            })
        );
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
        .single(); // Waits for the complete server response (DONE token)
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
        // Consume the items reactively instead of using a subscriber block
        .doOnNext(item -> {})
        .doOnError(error -> System.err.println("[" + stepName + "] Error: " + error.getMessage()))
        .then(); // Signals completion to the concatMap
  }
}