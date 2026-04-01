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
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsClientRandom {
  public static void main(String[] args) throws Exception {
    new TdsClientRandom().run();
  }

  private void run() throws Exception {
    ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(ConnectionFactoryOptions.DRIVER, "javatdslib")
        .option(HOST, "localhost")
        .option(PORT, 1433)
        .option(PASSWORD, "reactnonreact")
        .option(USER, "reactnonreact")
        .option(DATABASE, "reactnonreact")
        .option(TdsLibOptions.TRUST_SERVER_CERTIFICATE, true)
        .build());

    System.out.println("Connecting to database...");

    Mono.from(connectionFactory.create())
        // Use flatMap to chain the entire runSql operation
        .flatMap(this::runSql)
        .doOnError(t -> System.err.println("Connection/Run Failed: " + t.getMessage()))
        .block(); // Block once at the very end
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

                    int maxPossibleRows = Math.min(maxId, random.nextInt(5) + 1);
                    int numRows = random.nextInt(maxPossibleRows) + 1;

                    List<Integer> possibleIds = IntStream.rangeClosed(1, maxId).boxed().collect(Collectors.toList());
                    Collections.shuffle(possibleIds);
                    List<Integer> selectedIds = possibleIds.subList(0, Math.min(numRows, possibleIds.size()));

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
        )
        // 5. Close the connection after EVERYTHING completes
        .then(Mono.from(connection.close()))
        .doFinally(signal -> System.out.println("Connection safely closed."));
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