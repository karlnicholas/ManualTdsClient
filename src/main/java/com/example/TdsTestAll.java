package com.example;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.tdslib.javatdslib.api.TdsLibOptions;
import reactor.core.publisher.Mono;

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

    System.out.println("Connecting to database for TdsTestAll Bulk Execution...");

    UUID traceId = UUID.randomUUID();

    Mono.usingWhen(
            Mono.from(connectionFactory.create()),
            conn -> executeAllTests(conn, traceId),
            conn -> Mono.from(conn.close())
                .doOnSuccess(v -> System.out.println("\nAll tests finished. Connection safely closed."))
        )
        .doOnError(t -> System.err.println("\n❌ Test Suite Failed: " + t.getMessage()))
        .block();
  }

  private Mono<Void> executeAllTests(Connection conn, UUID traceId) {
    // 1. TdsClient MUST run first and complete successfully
    return runTest("TdsClient (Primary Setup)", () -> new TdsClient().runSql(conn, traceId))

        // 2. Execute the rest sequentially
        .then(runTest("TdsAllDataTypesTest", () -> new TdsAllDataTypesTest().runSql(conn)))
        .then(runTest("TdsClientBindingMatrixSymmetry", () -> new TdsClientBindingMatrixSymmetry().runSql(conn)))
        .then(runTest("TdsClientErrorTest", () -> new TdsClientErrorTest().runSql(conn)))
        .then(runTest("TdsClientExhaustiveNonNumericTest", () -> new TdsClientExhaustiveNonNumericTest().runSql(conn)))
        .then(runTest("TdsClientExhaustiveNumericTest", () -> new TdsClientExhaustiveNumericTest().runSql(conn)))
        .then(runTest("TdsClientFilterTest", () -> new TdsClientFilterTest().runSql(conn)))
        .then(runTest("TdsClientLobBug", () -> new TdsClientLobBug().runSql(conn)))
        .then(runTest("TdsClientLobTest", () -> new TdsClientLobTest().runSql(conn)))
        .then(runTest("TdsClientOrderSyncLobTest", () -> new TdsClientOrderSyncLobTest().runSql(conn)))
        .then(runTest("TdsClientOrderSyncTest", () -> new TdsClientOrderSyncTest().runSql(conn)))
        .then(runTest("TdsClientRandomAsync", () -> new TdsClientRandomAsync().runSql(conn)))
//        .then(runTest("TdsClientRandomPool", () -> new TdsClientRandomPool().runSql(conn)))
        .then(runTest("TdsClientSelectOne", () -> new TdsClientSelectOne().runSql(conn)))
        .then(runTest("TdsClientTestOutParams", () -> new TdsClientTestOutParams().runSql(conn)))
        .then(runTest("TdsClientTestSelect", () -> new TdsClientTestSelect().runSql(conn)))
        .then(runTest("TdsClientTypeMatrix", () -> new TdsClientTypeMatrix().runSql(conn)))
        .then(runTest("TdsClientXmlStream", () -> new TdsClientXmlStream().runSql(conn)))
        .then(runTest("TdsTransactionTestSimple", () -> new TdsTransactionTestSimple().runSql(conn)));
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