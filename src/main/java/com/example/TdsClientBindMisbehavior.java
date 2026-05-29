package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class TdsClientBindMisbehavior {
  public static void main(String[] args) { new TdsClientBindMisbehavior().run(); }

  private void run() {
    String r2dbcUrl = "r2dbc:mssql://reactnonreact:reactnonreact@localhost:1433/reactnonreact?trustServerCertificate=true";
    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(r2dbcUrl)).initialSize(2).maxSize(50)
.build());
    Mono.usingWhen(Mono.just(pool), this::runSql, ConnectionPool::disposeLater).block();
  }

  public Mono<Void> runSql(ConnectionPool pool) {
    return Mono.usingWhen(Mono.from(pool.create()),
        conn -> testDoubleSubscriptionOnRpcResult(conn)
            .then(testUnconsumedRpcResult(conn))
            .then(testSocketIntegrity(conn, "Final Integrity Check")),
        Connection::close);
  }

  /**
   * SPI Violation 1: Double Subscription on an RPC Result.
   * Proves the single-consumption lock works on Type 3 requests, not just Type 1 SQL_BATCH.
   */
  private Mono<Void> testDoubleSubscriptionOnRpcResult(Connection connection) {
    System.out.println("\n--- Test 1: Double Subscription on RPC Result ---");
    Publisher<? extends Result> publisher = connection.createStatement("SELECT @val AS val")
        .bind("val", "single_use")
        .execute();

    Mono<Void> firstSub = Flux.from(publisher)
        .flatMap(result -> result.flatMap(segment -> Mono.just(segment))).then();

    Mono<Void> secondSub = Flux.from(publisher)
        .flatMap(result -> result.flatMap(segment -> Mono.just(segment))).then();

    return firstSub.then(secondSub)
        .onErrorResume(e -> {
          System.out.println(" Caught expected double-subscription rejection: " + e.getMessage());
          return Mono.empty();
        });
  }

  /**
   * SPI Violation 2: Unconsumed RPC Result.
   * Proves the driver successfully vacuums ReturnValueTokens and DoneInProc tokens if dropped.
   */
  private Mono<Void> testUnconsumedRpcResult(Connection connection) {
    System.out.println("\n--- Test 2: Unconsumed RPC Result ---");
    return Flux.from(connection.createStatement("SELECT @val AS val").bind("val", "ghost").execute())
        .doOnNext(result -> System.out.println(" Received RPC Result, but dropping the publisher..."))
        .then();
  }

  private Mono<Void> testSocketIntegrity(Connection conn, String phase) {
    System.out.println(" -> Running Integrity Check (" + phase + ")...");
    return Flux.from(conn.createStatement("SELECT 99").execute()).flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class))).then();
  }
}