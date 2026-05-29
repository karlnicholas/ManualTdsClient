package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class TdsClientLobOrderSync {
  public static void main(String[] args) {
    new TdsClientLobOrderSync().run();
  }

  private void run() {
    String r2dbcUrl = "r2dbc:mssql://reactnonreact:reactnonreact@localhost:1433/reactnonreact?trustServerCertificate=true";
    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(r2dbcUrl)).initialSize(2).maxSize(50)
.build());

    System.out.println("Connecting to pool for Comprehensive Binding Matrix & Way Testing...");

    // Manage the Pool lifecycle and fail-fast on errors
    Mono.usingWhen(
            Mono.just(pool),
            this::runSql,
            p -> p.disposeLater().doOnSuccess(v -> System.out.println("\nTests complete. Connection pool closed."))
        )
        .doOnError(t -> System.err.println("\n❌ Test Suite Failed: " + t.getMessage()))
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

  public Mono<Void> runSql(Connection connection) {
    // 1. Discover columns
    // A. Random number of columns
    String query = "SELECT test_varchar_max, test_nvarchar_max FROM dbo.AllDataTypes where id=1";

    return executeQuery("Test", connection.createStatement(query).execute());
  }


  private Mono<Void> executeQuery(String stepName, Publisher<? extends Result> resultPublisher) {
    return Flux.from(resultPublisher)
        .flatMap(result -> result.map((row, meta) -> {
          StringBuilder sb = new StringBuilder();
          // Extract variables as requested
          Object value1 = row.get(0, String.class);
          Object value0 = row.get(1, String.class);
          return sb.toString().trim();
        }))
        .doOnError(error -> System.err.println("[" + stepName + "] Error: " + error.getMessage()))
        .then(); // Converts the Flux to a Mono<Void> signaling completion
  }
}