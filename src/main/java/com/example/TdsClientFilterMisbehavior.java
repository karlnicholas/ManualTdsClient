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

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class TdsClientFilterMisbehavior {
  public static void main(String[] args) {
    new TdsClientFilterMisbehavior().run();
  }

  private void run() {
    ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(DRIVER, "javatdslib").option(HOST, "localhost").option(PORT, 1433)
        .option(USER, "reactnonreact").option(PASSWORD, "reactnonreact").option(DATABASE, "reactnonreact")
        .option(TdsLibOptions.TRUST_SERVER_CERTIFICATE, true).build());

    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(connectionFactory)
        .initialSize(2).maxSize(10).maxIdleTime(Duration.ofMinutes(10)).build());

    Mono.usingWhen(Mono.just(pool), this::runSuite, p -> p.disposeLater()).block();
  }

  public Mono<Void> runSuite(ConnectionPool pool) {
    return Mono.usingWhen(
        Mono.from(pool.create()),
        connection -> testDoubleSubscriptionOnFilteredResult(connection)
            .then(testUnconsumedFilteredResult(connection))
            .then(testSocketIntegrity(connection, "Final Integrity Check")),
        Connection::close
    );
  }

  /**
   * SPI Violation 1: The R2DBC spec requires Publishers to reject multiple subscriptions.
   * This tests if the driver correctly rejects double-subscriptions even AFTER a filter is applied.
   */
  private Mono<Void> testDoubleSubscriptionOnFilteredResult(Connection connection) {
    System.out.println("\n--- Test 1: Illegal Double Subscription on Filtered Result ---");

    return Flux.from(connection.createStatement("SELECT 'single_use' AS val").execute())
        .flatMap(result -> {
          // Apply the filter, which returns a new Result instance
          Result filteredResult = result.filter(segment -> true);

          // Sub 1
          Mono<Void> firstSub = Flux.from(filteredResult.flatMap(segment -> Mono.just(segment))).then();

          // Sub 2
          Mono<Void> secondSub = Flux.from(filteredResult.flatMap(segment -> Mono.just(segment))).then();

          return firstSub.then(secondSub);
        })
        .then() // Convert the outer Flux<Void> back to Mono<Void> to satisfy compilation
        .onErrorResume(e -> {
          System.out.println(" Caught expected double-subscription rejection: " + e.getMessage());
          return Mono.empty();
        });
  }

  /**
   * SPI Violation 2: The client executes, applies a filter, but NEVER subscribes to the segments.
   * Tests if the driver leaves ghost tokens on the wire if the filtered stream is abandoned.
   */
  private Mono<Void> testUnconsumedFilteredResult(Connection connection) {
    System.out.println("\n--- Test 2: Unconsumed Filtered Result ---");
    return Flux.from(connection.createStatement("SELECT 'ghost' AS val").execute())
        .doOnNext(result -> {
          // We apply the filter, but we deliberately drop the returned Result.
          result.filter(segment -> true);
          System.out.println(" Applied filter, but dropping the publisher...");
        })
        .then();
  }

  private Mono<Void> testSocketIntegrity(Connection connection, String phase) {
    System.out.println(" -> Running Integrity Check (" + phase + ")...");
    return Flux.from(connection.createStatement("SELECT 99 AS test_val;").execute())
        .flatMap(result -> result.map((row, meta) -> row.get(0, Integer.class)))
        .doOnNext(val -> System.out.println("    [OK] Integrity Check Passed: " + val))
        .then();
  }
}