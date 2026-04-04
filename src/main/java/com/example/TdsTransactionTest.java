package com.example;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.api.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsTransactionTest {

  public static void main(String[] args) {
    new TdsTransactionTest().run();
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

    System.out.println("Connecting to database for Transaction Testing...");

    Mono.from(connectionFactory.create())
        .flatMap(this::runTxnTests)
        .doOnError(t -> System.err.println("Connection/Run Failed: " + t.getMessage()))
        .block();
  }

  private Mono<Void> runTxnTests(Connection connection) {
    String setupSql = """
        DROP TABLE IF EXISTS dbo.TxnTest;
        CREATE TABLE dbo.TxnTest (id INT, val INT);
        INSERT INTO dbo.TxnTest (id, val) VALUES (1, 100);
        """;

    return Mono.defer(() -> executeStream("0. Setup Temporary Table", connection.createStatement(setupSql).execute(), Result::getRowsUpdated))

        // --- TEST 1: COMMIT ---
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

        // Teardown
        .then(Mono.defer(() -> Mono.from(connection.close())))
        .doFinally(signal -> System.out.println("\nConnection safely closed."));
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