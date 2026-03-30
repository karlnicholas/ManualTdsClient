package com.example;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.api.TdsConnectionFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
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
        .option(TdsConnectionFactory.TRUST_SERVER_CERTIFICATE, true)
        .build());

    System.out.println("Connecting to database...");

    Mono.from(connectionFactory.create())
        .doOnNext(conn -> {
          try {
            runSql(conn);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        })
        .doOnError(t -> System.err.println("Connection/Run Failed: " + t.getMessage()))
        .block();
  }

  private void runSql(Connection connection) throws InterruptedException {
    // 1. Discover columns
    List<String> allColumns = fetchColumnNames(connection);

    // 2. Discover max id (assuming id is int and sequential-ish)
    int maxId = fetchMaxId(connection);

    System.out.println("Found max id = " + maxId);

    Random random = new Random();

    // === LOOP: Generate many random queries ===
    for (int i = 1; i <= 6000; i++) {   // ← feel free to change count
      // A. Random number of columns
      int numColumns = random.nextInt(allColumns.size()) + 1;

      List<String> selectedColumns = new ArrayList<>(allColumns);
      Collections.shuffle(selectedColumns);
      selectedColumns = selectedColumns.subList(0, numColumns);

      String selectList = String.join(", ", selectedColumns);

      // B. Random number of rows + random IDs
      int maxPossibleRows = Math.min(maxId, random.nextInt(5) + 1); // 1–3 rows most of the time
      int numRows = random.nextInt(maxPossibleRows) + 1;

      List<Integer> possibleIds = IntStream.rangeClosed(1, maxId)
          .boxed().collect(Collectors.toList());
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

//      System.out.println("--- Query " + i + " → " + numColumns + " cols, " + numRows + " rows ---");
//      System.out.println(dynamicQuery);   // uncomment for debugging
      if ( i % 1000 == 0) {
        System.out.println(i);
      }

      String stepName = "Random Query #" + i + " (" + numColumns + " cols, " + numRows + " rows)";

      executeRandomQuery(stepName, connection.createStatement(dynamicQuery).execute(), selectedColumns);
    }

    Mono.from(connection.close())
        .doFinally(signal -> System.out.println("Connection safely closed."))
        .subscribe();
  }

  private List<String> fetchColumnNames(Connection connection) throws InterruptedException {
    System.out.println("Fetching column list from INFORMATION_SCHEMA...");

    List<String> columns = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);

    String sql = """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dbo'
              AND TABLE_NAME = 'AllDataTypes'
            ORDER BY ORDINAL_POSITION;
            """;

    Flux.from(connection.createStatement(sql).execute())
        .flatMap(result -> result.map((row, meta) -> row.get(0, String.class)))
        .subscribe(
            columns::add,
            error -> {
              System.err.println("Column fetch error: " + error.getMessage());
              latch.countDown();
            },
            () -> {
              System.out.println("Fetched " + columns.size() + " columns.");
              latch.countDown();
            }
        );

    latch.await();
    if (columns.isEmpty()) {
      throw new RuntimeException("No columns found in dbo.AllDataTypes!");
    }
    return columns;
  }

  private int fetchMaxId(Connection connection) throws InterruptedException {
    System.out.println("Fetching max(id) from AllDataTypes...");

    CountDownLatch latch = new CountDownLatch(1);
    final int[] maxIdHolder = {0};

    String sql = "SELECT ISNULL(MAX(id), 0) FROM dbo.AllDataTypes";

    Flux.from(connection.createStatement(sql).execute())
        .flatMap(result -> result.map((row, meta) -> row.get(0, Integer.class)))
        .subscribe(
            value -> maxIdHolder[0] = value,
            error -> {
              System.err.println("Max ID fetch error: " + error.getMessage());
              latch.countDown();
            },
            () -> latch.countDown()
        );

    latch.await();
    return maxIdHolder[0];
  }

  private void executeRandomQuery(String stepName, Publisher<? extends Result> resultPublisher, List<String> columnOrder)
      throws InterruptedException {
//    System.out.println("\n--- " + stepName + " ---");
    CountDownLatch latch = new CountDownLatch(1);

    Flux.from(resultPublisher)
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
        .subscribe(
//            item -> System.out.println("  → " + item),
            item -> {},
            error -> {
              System.err.println("[" + stepName + "] Error: " + error.getMessage());
              latch.countDown();
            },
            () -> {
//              System.out.println("--- Done: " + stepName + " ---");
              latch.countDown();
            }
        );

    latch.await();
  }
}