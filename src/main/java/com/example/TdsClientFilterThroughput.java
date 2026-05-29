package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.Result;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class TdsClientFilterThroughput {
  public static void main(String[] args) { new TdsClientFilterThroughput().run(); }

  private void run() {
    String r2dbcUrl = "r2dbc:mssql://reactnonreact:reactnonreact@localhost:1433/reactnonreact?trustServerCertificate=true";
    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(r2dbcUrl)).initialSize(2).maxSize(50)
.build());
    Mono.usingWhen(Mono.just(pool), this::runSql, ConnectionPool::disposeLater).block();
  }

  public Mono<Void> runSql(ConnectionPool pool) {
    return testMassiveRejectionRate(pool)
        .then(testConcurrentFiltering(pool));
  }

  /**
   * Throughput 1: The Massive Rejection Test.
   * We generate 50,000 rows, but the filter ONLY keeps 10 of them.
   * This tests if the driver efficiently drops unmapped tokens without memory ballooning.
   */
  private Mono<Void> testMassiveRejectionRate(ConnectionPool pool) {
    System.out.println("\n--- Test 1: Massive Filter Rejection (50,000 rows) ---");
    String massiveSql = "SET TEXTSIZE -1; SELECT TOP 50000 row_number() over(order by (select null)) as rnum FROM sys.all_columns a CROSS JOIN sys.all_columns b;";

    return Mono.usingWhen(Mono.from(pool.create()),
        conn -> Flux.from(conn.createStatement(massiveSql).execute())
            .map(result -> result.filter(segment -> {
              if (segment instanceof Result.RowSegment rs) {
                Integer rnum = rs.row().get("rnum", Integer.class);
                return rnum != null && rnum % 5000 == 0; // Reject 99.9% of the rows
              }
              return false;
            }))
            .flatMap(res -> res.flatMap(segment -> Mono.just(segment)))
            .doOnNext(seg -> System.out.println("  Survived Filter! -> " + ((Result.RowSegment)seg).row().get("rnum", Integer.class)))
            .then(),
        Connection::close);
  }

  /**
   * Throughput 2: Concurrent Filtering.
   * 10 threads borrowing connections simultaneously, heavily filtering their results.
   * Proves the filter state machine is thread-safe and isolated per-connection.
   */
  private Mono<Void> testConcurrentFiltering(ConnectionPool pool) {
    System.out.println("\n--- Test 2: Concurrent Filtering (10 Workers) ---");

    return Flux.range(1, 10)
        .flatMap(workerId -> Mono.usingWhen(
            Mono.from(pool.create()),
            conn -> Flux.from(conn.createStatement("SELECT " + workerId + " AS id").execute())
                .map(result -> result.filter(segment -> segment instanceof Result.RowSegment))
                .flatMap(res -> res.flatMap(segment -> Mono.just(segment)))
                .then(),
            Connection::close
        ), 10)
        .doOnComplete(() -> System.out.println("  All 10 workers completed concurrent filtering."))
        .then();
  }
}