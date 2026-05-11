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

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsClientSelectOnlyMisbehavior {
  public static void main(String[] args) {
    new TdsClientSelectOnlyMisbehavior().run();
  }

  private void run() {
    ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(ConnectionFactoryOptions.DRIVER, "javatdslib")
        .option(HOST, "localhost")
        .option(PORT, 1433)
        .option(PASSWORD, "reactnonreact")
        .option(USER, "reactnonreact")
        .option(DATABASE, "reactnonreact")
        .option(TdsLibOptions.TRUST_SERVER_CERTIFICATE, true)
        .build());

    ConnectionPoolConfiguration poolConfiguration = ConnectionPoolConfiguration.builder(connectionFactory)
        .initialSize(2)
        .maxSize(10)
        .maxIdleTime(Duration.ofMinutes(10))
        .build();

    ConnectionPool pool = new ConnectionPool(poolConfiguration);

    System.out.println("Connecting to pool for Client Misbehavior Testing...");

    Mono.usingWhen(
            Mono.just(pool),
            this::runMisbehaviorSuite,
            p -> p.disposeLater().doOnSuccess(v -> System.out.println("\nTests complete. Connection pool closed."))
        )
        .doOnError(t -> System.err.println("\n❌ Test Suite Failed (Connection Dropped): " + t.getMessage()))
        .block();
  }

  public Mono<Void> runMisbehaviorSuite(ConnectionPool pool) {
    return Mono.usingWhen(
        Mono.from(pool.create()),
        connection ->
            testMapperException(connection)
                .then(testDoubleSubscription(connection))
                .then(testUnconsumedResult(connection))
                .then(testBlockingInMapper(connection))
                .then(testSocketIntegrity(connection, "Final Integrity Check")),
        Connection::close
    );
  }

  /**
   * Misbehavior 1: The application throws an exception (e.g., NPE) inside the row mapper.
   * Tests if the driver correctly intercepts the failure, fires an Attention signal, and vacuums the wire.
   */
  private Mono<Void> testMapperException(Connection connection) {
    System.out.println("\n--- Test 1: Exception Thrown in Mapper ---");
    return Flux.from(connection.createStatement("SELECT 1 AS val; SELECT 2 AS val;").execute())
        .flatMap(result -> result.map((row, meta) -> {
          Integer val = row.get(0, Integer.class);
          if (val != null && val == 1) {
            throw new NullPointerException("Simulated application crash during mapping!");
          }
          return val;
        }))
        .then()
        .onErrorResume(e -> {
          System.out.println(" Caught expected mapper exception: " + e.getMessage());
          return Mono.empty();
        })
        .then(testSocketIntegrity(connection, "Post-Mapper Exception Check"));
  }

  /**
   * Misbehavior 3: The application subscribes to the same Result Publisher twice.
   * R2DBC 1.0.0.RELEASE explicitly states Publishers returned by execute() should reject multiple subscriptions.
   */
  private Mono<Void> testDoubleSubscription(Connection connection) {
    System.out.println("\n--- Test 3: Illegal Double Subscription ---");
    Publisher<? extends Result> publisher = connection.createStatement("SELECT 'single_use' AS val").execute();

    Mono<Void> firstSub = Flux.from(publisher)
        .flatMap(result -> result.map((row, meta) -> row.get(0, String.class)))
        .then();

    Mono<Void> secondSub = Flux.from(publisher)
        .flatMap(result -> result.map((row, meta) -> row.get(0, String.class)))
        .then();

    return firstSub
        .then(secondSub)
        .onErrorResume(e -> {
          System.out.println(" Caught expected double-subscription rejection: " + e.getMessage());
          return Mono.empty();
        })
        .then(testSocketIntegrity(connection, "Post-Double Subscription Check"));
  }

  /**
   * Misbehavior 4: The application executes a query but drops the Result publisher without subscribing.
   * Tests if the driver's queue or state machine gets permanently wedged by a "ghost" request.
   */
  private Mono<Void> testUnconsumedResult(Connection connection) {
    System.out.println("\n--- Test 4: Unconsumed Execution Result ---");

    // We execute, but we never use flatMap to subscribe to the Result mapping.
    // We just consume the outer Publisher.
    return Flux.from(connection.createStatement("SELECT 'ghost' AS val").execute())
        .doOnNext(result -> System.out.println(" Received Result object, but ignoring its contents..."))
        .then()
        .then(testSocketIntegrity(connection, "Post-Unconsumed Result Check"));
  }

  /**
   * Misbehavior 5: The application performs a thread-blocking operation inside the reactive mapper.
   * Tests if the driver's event loop survives starvation (note: this usually causes latency, but shouldn't deadlock).
   */
  private Mono<Void> testBlockingInMapper(Connection connection) {
    System.out.println("\n--- Test 5: Thread Blocking inside Mapper ---");
    return Flux.from(connection.createStatement("SELECT 'blocker' AS val").execute())
        .flatMap(result -> result.map((row, meta) -> {
          try {
            System.out.println(" Sleeping for 500ms in mapper (Starving event loop)...");
            Thread.sleep(500);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          return row.get(0, String.class);
        }))
        .then()
        .then(testSocketIntegrity(connection, "Post-Blocking Mapper Check"));
  }

  /**
   * Helper to ensure the physical TCP connection is still clean, un-locked, and fully operational.
   */
  private Mono<Void> testSocketIntegrity(Connection connection, String phase) {
    return Mono.defer(() -> {
      System.out.println(" -> Running Integrity Check (" + phase + ")...");
      return Flux.from(connection.createStatement("SELECT 99 AS test_val;").execute())
          .flatMap(result -> result.map((row, meta) -> row.get(0, Integer.class)))
          .doOnNext(val -> {
            if (val == 99) {
              System.out.println("    [OK] Integrity Check Passed.");
            } else {
              System.err.println("    [FAIL] Received unexpected value: " + val);
            }
          })
          .then();
    });
  }
}