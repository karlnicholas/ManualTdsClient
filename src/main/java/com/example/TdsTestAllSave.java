package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.tdslib.javatdslib.api.TdsLibOptions;
import org.tdslib.javatdslib.impl.TdsConnection;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsTestAllSave {

  public static void main(String[] args) {
    new TdsTestAllSave().run();
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
        .initialSize(5)
        .maxSize(50)
        .maxIdleTime(Duration.ofMinutes(10))
        .build();

    ConnectionPool pool = new ConnectionPool(poolConfiguration);

    System.out.println("Booting Global Connection Pool for TdsTestAll Bulk Execution...");

    UUID traceId = UUID.randomUUID();

    // Manage the global pool lifecycle
    Mono.usingWhen(
            Mono.just(pool),
            p -> executeAllTests(p, traceId)
                // MOVED: Explicitly trigger the audit INSIDE the usingWhen scope,
                // so it runs BEFORE the pool is disposed.
                .then(Mono.defer(() -> auditPool(p, 50))),
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

//    return Mono.usingWhen(
//        Mono.from(pool.create()),
//        con->{
//          return runTest("TdsClientRandomAsync", () -> new TdsClientRandomAsync().runSql(con))
//              .then(runTest("TdsClientRandomAsync", () -> new TdsClientRandomAsync().runSql(con)));
//        },
//        Connection::close
//    );

    return runTest("TdsClientRandomPool", () -> new TdsClientRandomPool().runSql(pool));

//        return runTest("TdsClientRandomAsync", () -> new TdsClientRandomAsync().runSql(pool))
//        .then(runTest("TdsClientRandomPool", () -> new TdsClientRandomPool().runSql(pool)));

//        return runTest("TdsClientRandomPool", () -> new TdsClientRandomPool().runSql(pool))
//        .then(runTest("TdsClientRandomPool", () -> new TdsClientRandomPool().runSql(pool)));

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

  private Mono<Void> auditPool(ConnectionPool pool, int poolSize) {
    System.out.println("\n--- Starting Post-Test Pool Integrity Audit ---");
    System.out.println("Requesting " + poolSize + " concurrent connections to flush out poisoned sockets...");

    java.util.concurrent.atomic.AtomicLong grandTotalQueued = new java.util.concurrent.atomic.AtomicLong();
    java.util.concurrent.atomic.AtomicLong grandTotalComplete = new java.util.concurrent.atomic.AtomicLong();
    List<String> finalStates = java.util.Collections.synchronizedList(new java.util.ArrayList<>());

    // 1. CHECK OUT all 50 connections concurrently AND HOLD THEM HOSTAGE.
    // This physically prevents the pool from handing the same connection out twice.
    return Flux.range(1, poolSize)
        .flatMap(i -> Mono.from(pool.create()))
        .collectList() // Wait until we have all 50 physical connections in our hands
        .flatMap(connections -> {
          // 2. Now run the ping test on our 50 unique connections
          return Flux.fromIterable(connections)
              .flatMap(conn -> {
                TdsConnection nativeConn;
                if (conn instanceof io.r2dbc.spi.Wrapped<?> wrapped) {
                  nativeConn = (TdsConnection) wrapped.unwrap();
                } else {
                  nativeConn = (TdsConnection) conn;
                }

                return Flux.from(conn.createStatement("SELECT 1").execute())
                    .flatMap(result -> result.map((row, meta) -> row.get(0, Integer.class)))
                    .timeout(Duration.ofSeconds(2))
                    .doOnError(e -> System.err.println("  [!] HUNG! Timeout reached."))
                    .then()
                    .doOnSuccess(v -> {
                      long queued = nativeConn.getTransport().debuggingInformation.queuedCount.get();
                      long complete = nativeConn.getTransport().debuggingInformation.completeCallback.get();

                      grandTotalQueued.addAndGet(queued);
                      grandTotalComplete.addAndGet(complete);

                      finalStates.add(
                          "SPID: " + nativeConn.getTransport().getContext().getSpid() +
                              nativeConn.getTransport().debuggingInformation.toString()
                      );
                    })
                    .thenReturn(conn); // Pass connection down the chain
              })
              .then() // Wait for all 50 pings to finish
              // 3. Safely return all 50 connections back to the pool at once
              .then(Flux.fromIterable(connections)
                  .flatMap(conn -> Mono.from(conn.close()))
                  .then());
        })
        .doOnSuccess(v -> {
          System.out.println("--- Pool Audit Complete ---\n");
          finalStates.forEach(state -> System.out.println("GOOD DRIVER STATE: " + state));

          long totalQ = grandTotalQueued.get();
          long totalC = grandTotalComplete.get();
          long activePings = finalStates.size();

          System.out.println("\n========================================================");
          System.out.println("📊 FINAL AUDIT SUMMARY");
          System.out.println("========================================================");
          System.out.println("  Connections Audited:   " + activePings + " / " + poolSize);
          System.out.println("  GRAND TOTAL QUERIES:   " + totalQ);
          System.out.println("  Driver Completed:      " + totalC);
          System.out.println("  Active Audit Pings:    " + activePings);
          System.out.println("  --------------------------------------------------");
          System.out.println("  MATH CHECK:            (" + totalC + " + " + activePings + ") == " + totalQ);

          if (totalQ == (totalC + activePings)) {
            System.out.println("  STATUS:                ✅ PERFECT MATCH (Zero Leaks)");
          } else {
            System.out.println("  STATUS:                ❌ MISMATCH (Leak Detected)");
            System.out.println("  DIFFERENCE:            " + (totalQ - (totalC + activePings)) + " queries lost/stuck.");
          }
          System.out.println("========================================================\n");
        });
  }
}