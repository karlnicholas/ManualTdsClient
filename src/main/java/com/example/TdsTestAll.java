package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.tdslib.javatdslib.api.TdsLibOptions;
import org.tdslib.javatdslib.impl.TdsConnection;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
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

    ConnectionPoolConfiguration poolConfiguration = ConnectionPoolConfiguration.builder(connectionFactory)
        .initialSize(5)
        .maxSize(50)
        .maxIdleTime(Duration.ofMinutes(10))
        .build();

    ConnectionPool pool = new ConnectionPool(poolConfiguration);

    System.out.println("Booting Global Connection Pool for TdsTestAll Bulk Execution...");

    // Manage the global pool lifecycle
    Mono.usingWhen(
            Mono.just(pool),
            p -> executeAllTests(p)
                // AUDIT HAPPENS HERE: Inside the closure, after tests, before teardown.
                .then(Mono.defer(() -> auditPool(p, 50))),
            p -> p.disposeLater().doOnSuccess(v -> System.out.println("\nAll tests finished. Global Connection Pool safely closed."))
        )
        .doOnError(t -> System.err.println("\n❌ Test Suite Failed: " + t.getMessage()))
        .block();
  }

//  private Mono<Void> executeAllTests(ConnectionPool pool) {
//    // Both tests manage their own internal checkouts from the passed pool
//    return runTest("TdsClientRandomAsync", () -> new TdsClientRandomAsync().runSql(pool))
//        .then(runTest("TdsClientRandomPool", () -> new TdsClientRandomPool().runSql(pool)))
//        // 2. Execute the rest sequentially, passing the shared pool to each
//        .then(runTest("TdsAllDataTypesTest", () -> new TdsAllDataTypesTest().runSql(pool)))
//        .then(runTest("TdsClientBindingMatrixSymmetry", () -> new TdsClientBindingMatrixSymmetry().runSql(pool)))
//        .then(runTest("TdsClientErrorTest", () -> new TdsClientErrorTest().runSql(pool)))
//        .then(runTest("TdsClientExhaustiveNonNumericTest", () -> new TdsClientExhaustiveNonNumericTest().runSql(pool)))
//        .then(runTest("TdsClientExhaustiveNumericTest", () -> new TdsClientExhaustiveNumericTest().runSql(pool)))
//        .then(runTest("TdsClientFilterTest", () -> new TdsClientFilterTest().runSql(pool)))
//        .then(runTest("TdsClientLobBug", () -> new TdsClientLobBug().runSql(pool)))
//        .then(runTest("TdsClientLobTest", () -> new TdsClientLobTest().runSql(pool)))
//        .then(runTest("TdsClientOrderSyncLobTest", () -> new TdsClientOrderSyncLobTest().runSql(pool)))
//        .then(runTest("TdsClientOrderSyncTest", () -> new TdsClientOrderSyncTest().runSql(pool)))
//
//        .then(runTest("TdsClientSelectOne", () -> new TdsClientSelectOne().runSql(pool)))
//        .then(runTest("TdsClientTestOutParams", () -> new TdsClientTestOutParams().runSql(pool)))
//        .then(runTest("TdsClientTestSelect", () -> new TdsClientTestSelect().runSql(pool)))
//        .then(runTest("TdsClientTypeMatrix", () -> new TdsClientTypeMatrix().runSql(pool)))
//        .then(runTest("TdsClientXmlStream", () -> new TdsClientXmlStream().runSql(pool)))
//        .then(runTest("TdsTransactionTestSimple", () -> new TdsTransactionTestSimple().runSql(pool)));
//  }
private Mono<Void> executeAllTests(ConnectionPool pool) {
  System.out.println("🔥 TRIGGERING ALL TEST SUITES SIMULTANEOUSLY...");

  // Mono.when() subscribes to EVERY publisher in the list at once.
  // They will all compete for the 50 connections in the global pool.
  return Mono.when(
      // Batch 1: High Volume & LOBs
      runTest("CONCURRENT: RandomAsync", () -> new TdsClientRandomAsync().runSql(pool)),
      runTest("CONCURRENT: RandomPool", () -> new TdsClientRandomPool().runSql(pool)),
      runTest("CONCURRENT: LOB Stress", () -> new TdsClientLobOkTest().runSql(pool)),

      // Batch 2: Precision & State
      runTest("CONCURRENT: TypeMatrix", () -> new TdsClientTypeMatrix().runSql(pool)),
      runTest("CONCURRENT: XML Stream", () -> new TdsClientXmlStream().runSql(pool)),
      runTest("CONCURRENT: Transaction Logic", () -> new TdsTransactionTestSimple().runSql(pool)),
      runTest("CONCURRENT: Numeric Matrix", () -> new TdsClientExhaustiveNumericTest().runSql(pool)),

      // Batch 3: Edge Cases & Protocol Stress
      runTest("CONCURRENT: Error Recovery", () -> new TdsClientErrorTest().runSql(pool)),
      runTest("CONCURRENT: Filter Logic", () -> new TdsClientFilterTest().runSql(pool)),
      runTest("CONCURRENT: Binding Matrix", () -> new TdsClientBindingMatrixSymmetry().runSql(pool)),
      runTest("CONCURRENT: Non-Numeric Matrix", () -> new TdsClientExhaustiveNonNumericTest().runSql(pool)),
      runTest("CONCURRENT: AllDataTypes", () -> new TdsAllDataTypesTest().runSql(pool))
  );
}
//  private Mono<Void> executeAllTests(ConnectionPool pool) {
//    // Grouping into concurrent batches to stress different driver paths simultaneously
//
//    // Batch 1: The "Hammer" Batch (High-frequency random queries vs. Massive LOB streams)
//    Mono<Void> batch1 = Mono.when(
//        runTest("CONCURRENT: RandomAsync", () -> new TdsClientRandomAsync().runSql(pool)),
//        runTest("CONCURRENT: RandomPool", () -> new TdsClientRandomPool().runSql(pool)),
//        runTest("CONCURRENT: LOB Stress", () -> new TdsClientLobOkTest().runSql(pool))
//    );
//
//    // Batch 2: The "Precision" Batch (Matrix testing, Transactions, and XML streaming)
//    Mono<Void> batch2 = Mono.when(
//        runTest("CONCURRENT: TypeMatrix", () -> new TdsClientTypeMatrix().runSql(pool)),
//        runTest("CONCURRENT: XML Stream", () -> new TdsClientXmlStream().runSql(pool)),
//        runTest("CONCURRENT: Transaction Logic", () -> new TdsTransactionTestSimple().runSql(pool)),
//        runTest("CONCURRENT: Numeric Matrix", () -> new TdsClientExhaustiveNumericTest().runSql(pool))
//    );
//
//    // Batch 3: The "Edge Case" Batch (Errors, Binding Symmetry, and Filtering)
//    Mono<Void> batch3 = Mono.when(
//        runTest("CONCURRENT: Error Recovery", () -> new TdsClientErrorTest().runSql(pool)),
//        runTest("CONCURRENT: Filter Logic", () -> new TdsClientFilterTest().runSql(pool)),
//        runTest("CONCURRENT: Binding Matrix", () -> new TdsClientBindingMatrixSymmetry().runSql(pool))
//    );
//
//    // Execute the batches sequentially so the Audit remains deterministic
//    return batch1.then(batch2).then(batch3);
//  }

  private Mono<Void> runTest(String testName, Supplier<Mono<Void>> testExecution) {
    return Mono.defer(() -> {
      System.out.println("\n========================================================");
      System.out.println("🚀 RUNNING SUITE: " + testName + " " + LocalDateTime.now());
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
    java.util.concurrent.atomic.AtomicLong grandTotalError = new java.util.concurrent.atomic.AtomicLong();
    java.util.concurrent.atomic.AtomicLong grandTotalCancel = new java.util.concurrent.atomic.AtomicLong();
    List<String> finalStates = java.util.Collections.synchronizedList(new java.util.ArrayList<>());

    return Flux.range(1, poolSize)
        .flatMap(i -> Mono.from(pool.create()))
        .collectList()
        .flatMap(connections -> {
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
                      long error = nativeConn.getTransport().debuggingInformation.errorCallback.get();
                      long cancel = nativeConn.getTransport().debuggingInformation.cancelCallback.get();

                      grandTotalQueued.addAndGet(queued);
                      grandTotalComplete.addAndGet(complete);
                      grandTotalError.addAndGet(error);
                      grandTotalCancel.addAndGet(cancel);

                      finalStates.add(
                          "SPID: " + nativeConn.getTransport().getContext().getSpid() +
                              nativeConn.getTransport().debuggingInformation.toString()
                      );
                    })
                    .thenReturn(conn);
              })
              .then()
              .then(Flux.fromIterable(connections)
                  .flatMap(conn -> Mono.from(conn.close()))
                  .then());
        })
        .doOnSuccess(v -> {
          System.out.println("--- Pool Audit Complete ---\n");
          finalStates.forEach(state -> System.out.println("GOOD DRIVER STATE: " + state));

          long totalQ = grandTotalQueued.get();
          long totalC = grandTotalComplete.get();
          long totalE = grandTotalError.get();
          long totalX = grandTotalCancel.get();
          long activePings = finalStates.size();

          long accountedFor = totalC + totalE + totalX + activePings;

          System.out.println("\n========================================================");
          System.out.println("📊 FINAL AUDIT SUMMARY");
          System.out.println("========================================================");
          System.out.println("  Connections Audited:   " + activePings + " / " + poolSize);
          System.out.println("  GRAND TOTAL QUERIES:   " + totalQ);
          System.out.println("  --------------------------------------------------");
          System.out.println("  Driver Completed:      " + totalC);
          System.out.println("  Driver Errored:        " + totalE);
          System.out.println("  Driver Cancelled:      " + totalX);
          System.out.println("  Active Audit Pings:    " + activePings);
          System.out.println("  --------------------------------------------------");
          System.out.println("  MATH CHECK:            (" + totalC + " + " + totalE + " + " + totalX + " + " + activePings + ") == " + totalQ);

          if (totalQ == accountedFor) {
            System.out.println("  STATUS:                ✅ PERFECT MATCH (Zero Leaks)");
          } else {
            System.out.println("  STATUS:                ❌ MISMATCH (Leak Detected)");
            System.out.println("  DIFFERENCE:            " + (totalQ - accountedFor) + " queries lost/stuck.");
          }
          System.out.println("========================================================\n");

        });
  }
}