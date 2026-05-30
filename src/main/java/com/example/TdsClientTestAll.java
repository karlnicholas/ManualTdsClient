package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ValidationDepth;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class TdsClientTestAll {

  public static void main(String[] args) {
    new TdsClientTestAll().run();
  }

  private void run() {
    String r2dbcUrl = "r2dbc:mssql://reactnonreact:reactnonreact@localhost:1433/reactnonreact?trustServerCertificate=true";
    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(r2dbcUrl)).initialSize(2).maxSize(50)
.build());

    System.out.println("Booting Global Connection Pool for TdsClientTestAll Bulk Execution...");

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

  private Mono<Void> executeAllTests(ConnectionPool pool) {
    System.out.println("🔥 TRIGGERING TEST SUITES IN SEQUENTIAL GROUPS...");

    // Some of your clients might require the UUID traceId signature: runSql(pool, traceId).
    // If they only take (pool), you can remove the UUID generation.
    UUID traceId = UUID.randomUUID();

    // --- GROUP 1: ESSENTIAL ---
    Mono<Void> essentialGroup = Mono.defer(() -> {
      System.out.println("\n\n🟩 STARTING GROUP: ESSENTIAL 🟩");
      return runTest("TdsClientEssential", () -> new TdsClientEssential().runSql(pool, traceId));
    });

    // --- GROUP 2: BATCH ---
    Mono<Void> batchGroup = Mono.defer(() -> {
      System.out.println("\n\n🟦 STARTING GROUP: BATCH 🟦");
      return Mono.when(
          runTest("Batch", () -> new TdsClientBatch().runSql(pool, traceId)),
          runTest("BatchExceptions", () -> new TdsClientBatchExceptions().runSql(pool, traceId)),
          runTest("BatchHighThroughput", () -> new TdsClientBatchHighThroughput().runThroughputTest(pool, traceId)),
          runTest("BatchMisbehaved", () -> new TdsClientBatchMisbehaved().runSql(pool, traceId))
      );
    });

    // --- GROUP 3: BIND ---
    Mono<Void> bindGroup = Mono.defer(() -> {
      System.out.println("\n\n🟪 STARTING GROUP: BIND 🟪");
      return Mono.when(
          runTest("BindChaos", () -> new TdsClientBindChaos().runSql(pool)),
          runTest("BindChar", () -> new TdsClientBindChar().runSql(pool)),
//          runTest("BindEomParadox", () -> new TdsClientBindEomParadox().runSql(pool)),
          runTest("BindExceptions", () -> new TdsClientBindExceptions().runSql(pool)),
          runTest("BindMassiveRpc", () -> new TdsClientBindMassiveRpc().runSuite(pool))
//          runTest("BindMisbehavior", () -> new TdsClientBindMisbehavior().runSql(pool))
//          runTest("BindSpStress", () -> new TdsClientBindSpStress().runSql(pool))
//          runTest("BindThroughput", () -> new TdsClientBindThroughput().runSuite(pool))
      );
    });

    // --- GROUP 4: DATATYPE ---
    Mono<Void> dataTypeGroup = Mono.defer(() -> {
      System.out.println("\n\n🟨 STARTING GROUP: DATATYPE 🟨");
      return Mono.when(
          runTest("DataTypeAll", () -> new TdsClientDataTypeAll().runSql(pool)),
          runTest("DataTypeBindingMatrixSymmetry", () -> new TdsClientDataTypeBindingMatrixSymmetry().runSql(pool)),
          runTest("DataTypeNonNumeric", () -> new TdsClientDataTypeNonNumeric().runSql(pool)),
          runTest("DataTypeNumeric", () -> new TdsClientDataTypeNumeric().runSql(pool))
      );
    });

    // --- GROUP 5: FILTER ---
    Mono<Void> filterGroup = Mono.defer(() -> {
      System.out.println("\n\n🟧 STARTING GROUP: FILTER 🟧");
      return Mono.when(
          runTest("Filter", () -> new TdsClientFilter().runSql(pool)),
          runTest("FilterChaos", () -> new TdsClientFilterChaos().runSql(pool)),
          runTest("FilterExceptions", () -> new TdsClientFilterExceptions().runSql(pool)),
          runTest("FilterMisbehavior", () -> new TdsClientFilterMisbehavior().runSql(pool)),
          runTest("FilterThroughput", () -> new TdsClientFilterThroughput().runSql(pool))
      );
    });

    // --- GROUP 6: LOB ---
    Mono<Void> lobGroup = Mono.defer(() -> {
      System.out.println("\n\n🟥 STARTING GROUP: LOB 🟥");
      return Mono.when(
          runTest("LobBinding", () -> new TdsClientLobBinding().runSql(pool)),
          runTest("LobBindingStatementAdd", () -> new TdsClientLobBindingStatementAdd().runSql(pool)),
          runTest("LobChaos", () -> new TdsClientLobChaos().runSql(pool)),
          runTest("LobExtraction", () -> new TdsClientLobExtraction().runSql(pool)),
          runTest("LobOrderSync", () -> new TdsClientLobOrderSync().runSql(pool))
      );
    });

    // --- GROUP 7: ORDER ---
    Mono<Void> orderGroup = Mono.defer(() -> {
      System.out.println("\n\n🟫 STARTING GROUP: ORDER 🟫");
      return Mono.when(
          runTest("Order", () -> new TdsClientOrder().runSql(pool))
//          runTest("OrderSync", () -> new TdsClientOrderSync().runSql(pool))
      );
    });

    // --- GROUP 8: RANDOM ---
    Mono<Void> randomGroup = Mono.defer(() -> {
      System.out.println("\n\n⬛ STARTING GROUP: RANDOM ⬛");
      return Mono.when(
          runTest("RandomBaseline", () -> new TdsClientRandomBaseline().runSql(pool)),
          runTest("RandomPool", () -> new TdsClientRandomPool().runSql(pool)),
          runTest("RandomSequential", () -> new TdsClientRandomSequential().runSql(pool))
      );
    });

    // --- GROUP 9: SELECT ONLY ---
    Mono<Void> selectOnlyGroup = Mono.defer(() -> {
      System.out.println("\n\n⬜ STARTING GROUP: SELECT ONLY ⬜");
      return Mono.when(
          runTest("SelectOnlyChaos", () -> new TdsClientSelectOnlyChaos().runSql(pool)),
          runTest("SelectOnlyExceptions", () -> new TdsClientSelectOnlyExceptions().runSql(pool)),
//          runTest("SelectOnlyMacroChaos", () -> new TdsClientSelectOnlyMacroChaos().runSql(pool)),
          runTest("SelectOnlyMicroPoison", () -> new TdsClientSelectOnlyMicroPoison().runSql(pool)),
          runTest("SelectOnlyMisbehavior", () -> new TdsClientSelectOnlyMisbehavior().runSql(pool))
      );
    });

    // --- GROUP 10: TRANSACTION ---
    Mono<Void> transactionGroup = Mono.defer(() -> {
      System.out.println("\n\n🟩 STARTING GROUP: TRANSACTION 🟩");
      return Mono.when(
          runTest("Transaction", () -> new TdsClientTransaction().runSql(pool)),
          runTest("TransactionExceptions", () -> new TdsClientTransactionExceptions().runSql(pool)),
          runTest("TransactionHighThroughput", () -> new TdsClientTransactionHighThroughput().runThroughputTest(pool)),
          runTest("TransactionMisbehaved", () -> new TdsClientTransactionMisbehaved().runSql(pool)),
          runTest("TransactionSimple", () -> new TdsClientTransactionSimple().runSql(pool))
      );
    });

    // --- GROUP 11: MISC/ORPHANS ---
    Mono<Void> miscGroup = Mono.defer(() -> {
      System.out.println("\n\n🟦 STARTING GROUP: MISC/ORPHANS 🟦");
      return Mono.when(
          runTest("SqlChaos", () -> new TdsClientSqlChaos().runSql(pool)),
          runTest("StatementAddChaos", () -> new TdsClientStatementAddChaos().runSql(pool)),
          runTest("TestOutParams", () -> new TdsClientTestOutParams().runSql(pool)),
          runTest("TypeMatrix", () -> new TdsClientTypeMatrix().runSql(pool))
      );
    });

    // Execute the groups sequentially
    return
        essentialGroup
        .then(batchGroup)
        .then(bindGroup)
        .then(dataTypeGroup)
        .then(filterGroup)
        .then(lobGroup)
        .then(orderGroup)
        .then(randomGroup)
        .then(selectOnlyGroup)
        .then(transactionGroup)
        .then(miscGroup)
        ;
  }

  private Mono<Void> runTest(String testName, Supplier<Mono<Void>> testExecution) {
    return Mono.defer(() -> {
      System.out.println("\n========================================================");
      System.out.println("🚀 QUEUED SUITE: " + testName + " " + LocalDateTime.now());
      System.out.println("========================================================");

      return testExecution.get()
          .doOnSuccess(v -> System.out.println("✅ COMPLETED: " + testName));
    });
  }

  private Mono<Void> auditPool(ConnectionPool pool, int poolSize) {
    System.out.println("\n--- Starting Post-Test Pool Integrity Audit ---");
    System.out.println("Requesting " + poolSize + " concurrent connections to flush out poisoned sockets...");

    List<String> finalStates = java.util.Collections.synchronizedList(new java.util.ArrayList<>());

    return Flux.range(1, poolSize)
        .flatMap(i -> Mono.from(pool.create()))
        .collectList()
        .flatMap(connections -> {
          return Flux.fromIterable(connections)
              .flatMap(conn -> {
                // Use R2DBC's native validate method with a REMOTE depth
                return Mono.from(conn.validate(ValidationDepth.REMOTE))
                    .timeout(Duration.ofSeconds(2))
                    .doOnSuccess(isValid -> {
                      if (Boolean.TRUE.equals(isValid)) {
                        finalStates.add("VALID");
                      } else {
                        System.err.println("  [!] Connection reported as INVALID by driver.");
                      }
                    })
                    .doOnError(e -> System.err.println("  [!] HUNG or ERROR! Timeout/failure reached: " + e.getMessage()))
                    // Swallow the error so we don't break the Flux and skip closing the other connections
                    .onErrorResume(e -> Mono.empty())
                    .thenReturn(conn);
              })
              .then()
              // Clean up all connections safely
              .then(Flux.fromIterable(connections)
                  .flatMap(conn -> Mono.from(conn.close()).onErrorResume(e -> Mono.empty()))
                  .then());
        })
        .doOnSuccess(v -> {
          System.out.println("--- Pool Audit Complete ---\n");
          finalStates.forEach(state -> System.out.println("GOOD DRIVER STATE: " + state));

          long activePings = finalStates.size();

          System.out.println("\n========================================================");
          System.out.println("📊 FINAL AUDIT SUMMARY");
          System.out.println("========================================================");
          System.out.println("  Connections Audited:   " + activePings + " / " + poolSize);
          System.out.println("  --------------------------------------------------");
          System.out.println("========================================================\n");
        });
  }
}