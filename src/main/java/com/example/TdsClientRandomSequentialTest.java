package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TdsClientRandomSequentialTest {
  public static void main(String[] args) throws Exception {
    new TdsClientRandomSequentialTest().run();
  }

  private void run() {
    String r2dbcUrl = "r2dbc:mssql://reactnonreact:reactnonreact@localhost:1433/reactnonreact?trustServerCertificate=true";
    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(r2dbcUrl)).initialSize(1).maxSize(1).build());

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

  public Mono<Void> runSql(ConnectionPool pool) {
    return Mono.usingWhen(
        Mono.from(pool.create()),
        this::runSql,
        Connection::close
    );
  }

  private Mono<Void> runSql(Connection connection) {
    return fetchColumnNames(connection)
        .flatMap(allColumns -> fetchMaxId(connection)
            .flatMap(maxId -> {
              System.out.println("Found max id = " + maxId);
              Random random = new Random();

              return Flux.range(1, 6000)
                  .concatMap(i -> {
                    int numColumns = random.nextInt(allColumns.size()) + 1;

                    List<String> shuffledColumns = new ArrayList<>(allColumns);
                    Collections.shuffle(shuffledColumns);
                    List<String> selectedColumns = shuffledColumns.subList(0, numColumns);

                    String selectList = String.join(", ", selectedColumns);
                    int numRows = Math.min(maxId, 20);

                    List<Integer> possibleIds = IntStream.rangeClosed(1, maxId).boxed().collect(Collectors.toList());
                    Collections.shuffle(possibleIds);
                    List<Integer> selectedIds = possibleIds.subList(0, numRows);

                    // Build the parameterized WHERE clause
                    String whereClause;
                    if (selectedIds.isEmpty()) {
                      whereClause = "WHERE 1=0";
                    } else {
                      String markers = IntStream.range(0, selectedIds.size())
                          .mapToObj(idx -> "@P" + idx)
                          .collect(Collectors.joining(", "));
                      whereClause = "WHERE id IN (" + markers + ")";
                    }

                    String dynamicQuery = "SELECT " + selectList +
                        "\nFROM dbo.AllDataTypes\n" + whereClause + "\nORDER BY id;";

                    if (i % 1000 == 0) {
                      System.out.println(i);
                    }

                    String stepName = "Random Query #" + i + " (" + numColumns + " cols, " + numRows + " rows)";

                    return Mono.defer(() -> {
                      Statement statement = connection.createStatement(dynamicQuery);

                      // Bind each generated parameter if the list isn't empty
                      if (!selectedIds.isEmpty()) {
                        for (int j = 0; j < selectedIds.size(); j++) {
                          // Standard R2DBC parameter binding format for SQL Server
                          statement.bind("P" + j, selectedIds.get(j));
                        }
                      }

                      return executeRandomQuery(stepName, statement.execute(), selectedColumns);
                    });
                  })
                  .then();
            })
        );
  }

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
          // BARE METAL PARSING:
          for (int idx = 0; idx < columnOrder.size(); idx++) {
            row.get(idx, Object.class);
          }
          return 1;
        }))
        .doOnNext(item -> {})
        .doOnError(error -> System.err.println("[" + stepName + "] Error: " + error.getMessage()))
        .then();
  }
}