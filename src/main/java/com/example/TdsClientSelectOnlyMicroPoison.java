package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.tdslib.r2dbc.mssql.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsClientSelectOnlyMicroPoison {
  public static void main(String[] args) {
    new TdsClientSelectOnlyMicroPoison().run();
  }

  private void run() {
    ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(ConnectionFactoryOptions.DRIVER, "mssql")
        .option(HOST, "localhost")
        .option(PORT, 1433)
        .option(PASSWORD, "reactnonreact")
        .option(USER, "reactnonreact")
        .option(DATABASE, "reactnonreact")
        .option(TdsLibOptions.TRUST_SERVER_CERTIFICATE, true)
        .build());

    // Configure a simple pool for the standalone client execution
    ConnectionPoolConfiguration poolConfiguration = ConnectionPoolConfiguration.builder(connectionFactory)
        .initialSize(2)
        .maxSize(10)
        .maxIdleTime(Duration.ofMinutes(10))
        .build();

    ConnectionPool pool = new ConnectionPool(poolConfiguration);

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
    System.out.println("Starting Cancellation-Poison Test...");

    // Query 2 will intentionally fail at the database level
    return Flux.just("SELECT 1 AS val", "SELECT 1/0 AS val", "SELECT 2 AS val")
        .flatMap(query -> {
          System.out.println(" -> Dispatching: " + query);
          return Flux.from(connection.createStatement(query).execute())
              .flatMap(result -> result.map((row, meta) -> row.get(0, Integer.class)))
              .doOnNext(val -> System.out.println(" <- Result: " + val))
              .doOnError(e -> System.err.println(" [!] Error caught: " + e.getMessage()));
        }, 3) // Concurrency = 3 forces them all into the driver's internal queue instantly
        .then()
        // Swallow the crash at the top level so we don't drop the connection via usingWhen
        .onErrorResume(e -> {
          System.err.println("--- Stream Aborted Due to Error ---");
          return Mono.empty();
        })
        // Now, fire ONE more query sequentially to prove if the queue lock is permanently stuck
        .then(Mono.defer(() -> {
          System.out.println("\n--- Testing Socket Integrity ---");
          return Flux.from(connection.createStatement("SELECT 99 AS test_val;").execute())
              .flatMap(result -> result.map((row, meta) -> row.get(0, Integer.class)))
              .doOnNext(val -> System.out.println(" <- Integrity Check Passed: " + val))
              .then();
        }));
  }
}