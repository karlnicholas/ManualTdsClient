package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.TransactionDefinition;
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.api.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.Function;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsTransactionTestSimple {

  public static void main(String[] args) {
    new TdsTransactionTestSimple().run();
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
    String setupSql = """
        DROP TABLE IF EXISTS dbo.TxnTest;
        CREATE TABLE dbo.TxnTest (id INT, val INT);
        INSERT INTO dbo.TxnTest (id, val) VALUES (1, 100);
        """;

    return Mono.from(connection.createStatement(setupSql).execute())
        .flatMapMany(Result::getRowsUpdated)
        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 1. Transaction Commit Test ---");
          return Mono.from(connection.beginTransaction())
              .doOnSuccess(v -> System.out.println("  -> [BEGIN TRAN] executed"))
              .then(Mono.from(connection.createStatement("UPDATE dbo.TxnTest SET val = 200 WHERE id = 1").execute()))
              .flatMap(res -> Mono.from(res.getRowsUpdated()))
              .doOnNext(count -> System.out.println("  -> Updated " + count + " rows inside transaction."))
              .then(Mono.from(connection.commitTransaction()))
              .doOnSuccess(v -> System.out.println("  -> [COMMIT TRAN] executed"))
              .then(Mono.from(connection.createStatement("SELECT val FROM dbo.TxnTest WHERE id = 1").execute()))
              .flatMap(res -> Mono.from(res.map((row, meta) -> row.get(0, Integer.class))))
              .doOnNext(val -> {
                if (val == 200) System.out.println("  -> [PASS] Value after COMMIT is 200");
                else System.out.println("  -> [FAIL] Value is " + val + " (Expected 200)");
              })
              .then();
        }))

        // --- TEST 2: ROLLBACK ---
        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 2. Transaction Rollback Test ---");
          return Mono.from(connection.beginTransaction())
              .doOnSuccess(v -> System.out.println("  -> [BEGIN TRAN] executed"))
              .then(Mono.from(connection.createStatement("UPDATE dbo.TxnTest SET val = 999 WHERE id = 1").execute()))
              .flatMap(res -> Mono.from(res.getRowsUpdated()))
              .doOnNext(count -> System.out.println("  -> Updated " + count + " rows inside transaction."))
              .then(Mono.from(connection.rollbackTransaction()))
              .doOnSuccess(v -> System.out.println("  -> [ROLLBACK TRAN] executed"))
              .then(Mono.from(connection.createStatement("SELECT val FROM dbo.TxnTest WHERE id = 1").execute()))
              .flatMap(res -> Mono.from(res.map((row, meta) -> row.get(0, Integer.class))))
              .doOnNext(val -> {
                if (val == 200) System.out.println("  -> [PASS] Value after ROLLBACK reverted to 200");
                else System.out.println("  -> [FAIL] Value is " + val + " (Expected 200)");
              })
              .then();
        }))

        // --- TEST 3: ISOLATION LEVEL & NAMED TRANSACTIONS ---
        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 3. TransactionDefinition Test (Serializable / Named) ---");

          TransactionDefinition customDef = new TransactionDefinition() {
            @Override
            public <T> T getAttribute(io.r2dbc.spi.Option<T> option) {
              if (option.equals(TransactionDefinition.ISOLATION_LEVEL)) {
                return (T) io.r2dbc.spi.IsolationLevel.SERIALIZABLE;
              }
              if (option.equals(TransactionDefinition.NAME)) {
                return (T) "TestSerializableTxn";
              }
              return null;
            }
          };

          return Mono.from(connection.beginTransaction(customDef))
              .doOnSuccess(v -> System.out.println("  -> [BEGIN TRAN] executed with SERIALIZABLE and Name"))
              .then(Mono.from(connection.createStatement(
                  "SELECT transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID"
              ).execute()))
              .flatMap(res -> Mono.from(res.map((row, meta) -> row.get(0, Short.class))))
              .doOnNext(isolationLevel -> {
                if (isolationLevel == 4) {
                  System.out.println("  -> [PASS] Server confirmed ISOLATION LEVEL is 4 (SERIALIZABLE)");
                } else {
                  System.out.println("  -> [FAIL] Server reports ISOLATION LEVEL is " + isolationLevel);
                }
              })
              .then(Mono.from(connection.rollbackTransaction()))
              .doOnSuccess(v -> System.out.println("  -> [ROLLBACK TRAN] executed"));
        }))

        // --- TEST 4: SEQUENTIAL TRANSACTIONS ---
        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 4. Sequential Transactions Test ---");
          return Mono.from(connection.beginTransaction())
              .doOnSuccess(v -> System.out.println("  -> [BEGIN TRAN 1] executed"))
              .then(Mono.from(connection.commitTransaction()))
              .doOnSuccess(v -> System.out.println("  -> [COMMIT TRAN 1] executed"))
              .then(Mono.from(connection.beginTransaction()))
              .doOnSuccess(v -> System.out.println("  -> [BEGIN TRAN 2] executed"))
              .then(Mono.from(connection.rollbackTransaction()))
              .doOnSuccess(v -> System.out.println("  -> [ROLLBACK TRAN 2] executed"))
              .doOnSuccess(v -> System.out.println("  -> [PASS] Successfully executed back-to-back transactions without protocol errors."));
        }))

        // --- TEST 5: STATE MACHINE VIOLATIONS ---
        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 5. State Machine Violation Test (Commit with no Txn) ---");
          return Mono.from(connection.commitTransaction())
              .doOnSuccess(v -> System.out.println("  -> [COMMIT TRAN] executed (expected no-op)"))
              .then(Mono.from(connection.rollbackTransaction()))
              .doOnSuccess(v -> System.out.println("  -> [ROLLBACK TRAN] executed (expected no-op)"))
              .doOnSuccess(v -> System.out.println("  -> [PASS] Handled commit/rollback without active transaction gracefully."));
        }))

        // --- TEST 6: ERROR DURING TRANSACTION ---
        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 6. Error Recovery Test ---");
          return Mono.from(connection.beginTransaction())
              .doOnSuccess(v -> System.out.println("  -> [BEGIN TRAN] executed"))
              .then(Mono.from(connection.createStatement("SELECT * FROM dbo.TableThatDoesNotExist").execute()))
              .flatMapMany(res -> res.map((row, meta) -> row.get(0)))
              .then()
              .onErrorResume(e -> {
                System.out.println("  -> Caught expected query error: " + e.getMessage());
                return Mono.empty();
              })
              .then(Mono.from(connection.rollbackTransaction()))
              .doOnSuccess(v -> System.out.println("  -> [ROLLBACK TRAN] executed successfully after error."))
              .doOnSuccess(v -> System.out.println("  -> [PASS] Connection remained usable and successfully rolled back."));
        }));
  }

  // --- Minimal Async Helper ---
  private <T> Mono<Void> executeStream(String stepName, Publisher<? extends Result> resultPublisher, Function<Result, Publisher<T>> extractor) {
    System.out.println("\n--- Executing: " + stepName + " ---");
    return Flux.from(resultPublisher)
        .flatMap(extractor)
        .doOnNext(item -> System.out.println("  -> Rows Affected/Output: " + item))
        .then();
  }
}