package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ValidationDepth;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class TdsClientSelectOnly {
  public static void main(String[] args) throws Exception {
    new TdsClientSelectOnly().run();
  }

  private void run() {
    String r2dbcUrl = "r2dbc:mssql://reactnonreact:reactnonreact@localhost:1433/reactnonreact?trustServerCertificate=true";

    // Constrained to exactly 1 connection.
    // Any leaked state or unreleased lock in the driver will instantly deadlock the application.
    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(r2dbcUrl))
        .initialSize(1).maxSize(1).build());

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
   * Borrows the single connection from the pool, runs the sequential pipeline,
   * and safely closes (releases) it when finished.
   */
  public Mono<Void> runSql(ConnectionPool pool) {
    return Mono.usingWhen(
        Mono.from(pool.create()),
        connection ->
            test_SimpleSelect(connection, "First"),
//                .then(test_SimpleSelect(connection, "Second"))
//                .then(checkConnection(connection)),
        Connection::close
    );
  }

  // --- CHAOS TEST 1: Sequential Execution ---
  private Mono<Void> test_SimpleSelect(Connection connection, String executionLabel) {
    // Mono.defer ensures NOTHING inside this block runs until .then() subscribes to it
    return Mono.defer(() -> {
      System.out.println("\n--- TEST: Simple Select (" + executionLabel + ") ---");
      return Flux.from(connection.createStatement("SELECT 1").execute())
          .flatMap(res -> res.map((row, metadata) -> row.get(0, Integer.class)))
          .doOnNext(val -> System.out.println("  [" + executionLabel + "] Read Value: " + val))
          .then();
    });
  }

  // --- CHAOS TEST 2: Network State Validation ---
  private Mono<Void> checkConnection(Connection connection) {
    // Mono.defer ensures this validation doesn't fire until both selects are completely finished
    return Mono.defer(() -> {
      System.out.println("\n--- TEST: Connection Validation ---");
      // Asks the driver to evaluate the physical socket health
      return Mono.from(connection.validate(ValidationDepth.REMOTE))
          .doOnNext(isValid -> System.out.println("  Connection is valid: " + isValid))
          .then();
    });
  }
}