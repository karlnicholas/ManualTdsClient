package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.tdslib.javatdslib.api.TdsLibOptions;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Supplier;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsTestAll {

  public static void main(String[] args) {
    new TdsTestAll().run();
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

// Configure the Global Pool for the entire Test Suite
    ConnectionPoolConfiguration poolConfiguration = ConnectionPoolConfiguration.builder(connectionFactory)
        .initialSize(5)
        .maxSize(50)
        .maxIdleTime(Duration.ofMinutes(10))
        // ADD THIS: If a test waits more than 15 seconds for a connection, crash loudly!
        .maxAcquireTime(Duration.ofSeconds(15))
        .build();

    ConnectionPool pool = new ConnectionPool(poolConfiguration);

    System.out.println("Booting Global Connection Pool for TdsTestAll Bulk Execution...");

    UUID traceId = UUID.randomUUID();

    // Manage the global pool lifecycle
    Mono.usingWhen(
            Mono.just(pool),
            p -> executeAllTests(p, traceId),
            p -> p.disposeLater().doOnSuccess(v -> System.out.println("\nAll tests finished. Global Connection Pool safely closed."))
        )
        .doOnError(t -> System.err.println("\n❌ Test Suite Failed: " + t.getMessage()))
        .block();
  }

  private Mono<Void> executeAllTests(ConnectionPool pool, UUID traceId) {
    // 1. TdsClient MUST run first and complete successfully
//    return runTest("TdsClient (Primary Setup)", () -> new TdsClient().runSql(pool, traceId))
//
//        // 2. Execute the rest sequentially, passing the shared pool to each
//        .then(runTest("TdsAllDataTypesTest", () -> new TdsAllDataTypesTest().runSql(pool)))
//        .then(runTest("TdsClientBindingMatrixSymmetry", () -> new TdsClientBindingMatrixSymmetry().runSql(pool)))
//        .then(runTest("TdsClientErrorTest", () -> new TdsClientErrorTest().runSql(pool)))
//        .then(runTest("TdsClientExhaustiveNonNumericTest", () -> new TdsClientExhaustiveNonNumericTest().runSql(pool)))
//        .then(runTest("TdsClientExhaustiveNumericTest", () -> new TdsClientExhaustiveNumericTest().runSql(pool)))
//        .then(runTest("TdsClientFilterTest", () -> new TdsClientFilterTest().runSql(pool)))
//        .then(runTest("TdsClientLobBug", () -> new TdsClientLobBug().runSql(pool)))
////        .then(runTest("TdsClientLobTest", () -> new TdsClientLobTest().runSql(pool)))
//        .then(runTest("TdsClientOrderSyncLobTest", () -> new TdsClientOrderSyncLobTest().runSql(pool)))
//        .then(runTest("TdsClientOrderSyncTest", () -> new TdsClientOrderSyncTest().runSql(pool)))
        return runTest("TdsClientRandomAsync", () -> new TdsClientRandomAsync().runSql(pool))
        .then(runTest("TdsClientRandomPool", () -> new TdsClientRandomPool().runSql(pool)));
//        .then(runTest("TdsClientSelectOne", () -> new TdsClientSelectOne().runSql(pool)))
//        .then(runTest("TdsClientTestOutParams", () -> new TdsClientTestOutParams().runSql(pool)))
//        .then(runTest("TdsClientTestSelect", () -> new TdsClientTestSelect().runSql(pool)))
//        .then(runTest("TdsClientTypeMatrix", () -> new TdsClientTypeMatrix().runSql(pool)))
//        .then(runTest("TdsClientXmlStream", () -> new TdsClientXmlStream().runSql(pool)))
//        .then(runTest("TdsTransactionTestSimple", () -> new TdsTransactionTestSimple().runSql(pool)));
  }

  /**
   * Helper method to inject clear console logging into the reactive pipeline
   * exactly when the test starts and ends.
   */
  private Mono<Void> runTest(String testName, Supplier<Mono<Void>> testExecution) {
    return Mono.defer(() -> {
      System.out.println("\n========================================================");
      System.out.println("🚀 RUNNING SUITE: " + testName);
      System.out.println("========================================================");

      return testExecution.get()
          .doOnSuccess(v -> System.out.println("✅ COMPLETED: " + testName));
    });
  }
}