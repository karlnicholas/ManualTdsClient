package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class TdsClientRandomPool {
  public static void main(String[] args) {
    new TdsClientRandomPool().run();
  }

  private void run() {

    String r2dbcUrl = "r2dbc:mssql://reactnonreact:reactnonreact@localhost:1433/reactnonreact?trustServerCertificate=true";
    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(r2dbcUrl)).initialSize(2).maxSize(50)
        .build());

    System.out.println("Connecting to database pool for Async Load Testing...");

    Mono.usingWhen(
            Mono.just(pool),
            this::runSql,
            conPool -> conPool.disposeLater().doOnSuccess(v -> System.out.println("\nConnection Pool safely closed."))
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
        .then();
  }
  // Define the limit (e.g., this test can only use 30 of the 50 connections)
//  private final java.util.concurrent.Semaphore poolLimit = new java.util.concurrent.Semaphore(30);
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
            System.out.println("Dispatched Random Pool Query #" + i);
          }

          String stepName = "Query #" + i + ":" + dynamicQuery;

//          return Mono.usingWhen(
//              // Only proceed if a permit is available
//              Mono.fromCallable(() -> {
//                poolLimit.acquire();
//                return i;
//              }).then(Mono.from(pool.create())),
//              conn -> executeRandomQuery(conn, stepName, dynamicQuery, conn.createStatement(dynamicQuery).execute(), selectedColumns),
//          conn -> Mono.from(conn.close())
//              .doFinally(s -> poolLimit.release()) // Release permit back to subset
//            );

          return Mono.usingWhen(
              Mono.from(pool.create()),
              conn -> executeRandomQuery(conn, stepName, dynamicQuery, conn.createStatement(dynamicQuery).execute(), selectedColumns),
              Connection::close
          );
        }, 20)
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

  private Mono<Void> executeRandomQuery(Connection connection, String stepName, String query, Publisher<? extends Result> resultPublisher, List<String> columnOrder) {
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
        .then();
  }
}