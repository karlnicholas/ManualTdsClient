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
    // 1. Define the base ConnectionFactory
    ConnectionFactory baseConnectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(ConnectionFactoryOptions.DRIVER, "javatdslib")
        .option(HOST, "localhost")
        .option(PORT, 1433)
        .option(PASSWORD, "reactnonreact")
        .option(USER, "reactnonreact")
        .option(DATABASE, "reactnonreact")
        .option(TdsLibOptions.TRUST_SERVER_CERTIFICATE, true)
        .build());

    // 2. Configure the ConnectionPool
    ConnectionPoolConfiguration poolConfiguration = ConnectionPoolConfiguration.builder(baseConnectionFactory)
        .initialSize(10)           // Start with 10 connections ready
        .maxSize(50)               // Scale up to 50 concurrent connections
        .maxIdleTime(Duration.ofMinutes(10))
        .maxCreateConnectionTime(Duration.ofSeconds(5))
        .build();

    // 3. Instantiate the Pool
    ConnectionPool connectionPool = new ConnectionPool(poolConfiguration);

    System.out.println("Connecting to database pool for Async Load Testing...");

    // 4. Manage the lifecycle of the pool itself
    Mono.usingWhen(
            Mono.just(connectionPool),
            this::runSql,
            pool -> pool.disposeLater().doOnSuccess(v -> System.out.println("\nConnection Pool safely closed."))
        )
        .doOnError(t -> System.err.println("Connection/Run Failed: " + t.getMessage()))
        .block();
  }

  private Mono<Void> runSql(ConnectionPool pool) {
    // Both setup methods now borrow a connection from the pool
    return fetchColumnNames(pool)
        .flatMap(allColumns -> fetchMaxId(pool)
            .flatMap(maxId -> {
              if (maxId <= 0) {
                return Mono.error(new IllegalStateException("Table is empty (maxId=0), cannot run tests."));
              }
              return executeLoadTest(pool, allColumns, maxId);
            })
        );
  }

  private Mono<Void> executeLoadTest(ConnectionPool pool, List<String> allColumns, int maxId) {
    System.out.println("Found max id = " + maxId + ". Starting async load test...");

    // Execute 6000 random queries asynchronously.
    // Concurrency is 256, but they will be mapped to a max of 50 physical connections via the pool.
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

          // Borrows a connection for the lifespan of this single query, then releases it
          return Mono.usingWhen(
              Mono.from(pool.create()),
              conn -> executeRandomQuery(stepName, conn.createStatement(dynamicQuery).execute(), selectedColumns),
              Connection::close
          );
        }, 256)
        .then();
  }

  // --- Latch-Free Helper Methods ---

  private Mono<List<String>> fetchColumnNames(ConnectionPool pool) {
    System.out.println("Fetching column list from INFORMATION_SCHEMA...");

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
        .doOnSuccess(columns -> System.out.println("Fetched " + columns.size() + " columns."))
        .filter(columns -> !columns.isEmpty())
        .switchIfEmpty(Mono.error(new RuntimeException("No columns found in dbo.AllDataTypes!")));
  }

  private Mono<Integer> fetchMaxId(ConnectionPool pool) {
    System.out.println("Fetching max(id) from AllDataTypes...");
    String sql = "SELECT ISNULL(MAX(id), 0) FROM dbo.AllDataTypes";

    return Mono.usingWhen(
        Mono.from(pool.create()),
        conn -> Flux.from(conn.createStatement(sql).execute())
            .flatMap(result -> result.map((row, meta) -> row.get(0, Integer.class)))
            .single(),
        Connection::close
    );
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
        .doOnNext(item -> {})
        .doOnError(error -> System.err.println("[" + stepName + "] Error: " + error.getMessage()))
        .then();
  }
}