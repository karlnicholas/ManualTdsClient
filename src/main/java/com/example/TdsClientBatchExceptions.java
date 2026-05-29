package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.ValidationDepth;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Function;

public class TdsClientBatchExceptions {

  public static void main(String[] args) {
    new TdsClientBatchExceptions().run();
  }

  private void run() {
    String r2dbcUrl = "r2dbc:mssql://reactnonreact:reactnonreact@localhost:1433/reactnonreact?trustServerCertificate=true";
    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(r2dbcUrl)).initialSize(2).maxSize(50)
.build());

    System.out.println("Connecting to database pool for Exception Translation Testing...");

    UUID traceId = UUID.randomUUID();

    Mono.usingWhen(
            Mono.just(pool),
            p -> runSql(p, traceId),
            p -> Mono.delay(Duration.ofSeconds(1)) // <-- Add a 1-second breather
                .then(p.disposeLater())
                .doOnSuccess(v -> System.out.println("\nConnection Pool safely closed."))
        )
        .doOnError(t -> System.err.println("\n❌ Test Suite Failed: " + t.getMessage()))
        .block();
  }

  public Mono<Void> runSql(ConnectionPool pool, UUID traceId) {
    return Mono.usingWhen(
        Mono.from(pool.create()),
        conn -> runSql(conn, traceId),
        Connection::close
    );
  }

  public Mono<Void> runSql(Connection connection, UUID traceId) {

    return Mono.defer(() -> {
          System.out.println("\n--- Executing: 1. Setup DDL ---");
          String setup = "DROP TABLE IF EXISTS dbo.ExceptionsTest; CREATE TABLE dbo.ExceptionsTest (id INT PRIMARY KEY);";
          return executeStream("1. Setup DDL", connection.createStatement(setup).execute(), Result::getRowsUpdated);
        })

        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 2. Bad Grammar Test ---");
          // Expects a syntax error / R2dbcBadGrammarException
          return executeExpectedErrorStream("2. Bad Grammar Test", connection.createStatement("SELCT * FROM dbo.ExceptionsTest").execute(), Result::getRowsUpdated);
        }))

        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 3. Divide by Zero Test (Reactor NPE) ---");
          // Expects a NullPointerException because Reactor map() rejects the NULL row emitted before the database error token
          return executeExpectedErrorStream("3. Divide by Zero Test", connection.createStatement("SELECT 1 / 0").execute(), res -> res.map((r, m) -> r.get(0)));
        }))

        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 3.5. Divide by Zero Test (Native Error) ---");
          // Bypasses the row mapping to catch the actual native TDS arithmetic exception
          return executeExpectedErrorStream("3.5. Divide by Zero Test (Native)", connection.createStatement("SET ARITHABORT ON; SELECT 1 / 0").execute(), Result::getRowsUpdated);
        }))

        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 4. Constraint Violation Test ---");
          // Insert row 1, then try to insert row 1 again to trigger a Primary Key violation (R2dbcDataIntegrityViolationException)
          return Flux.from(connection.createStatement("INSERT INTO dbo.ExceptionsTest (id) VALUES (1)").execute())
              .flatMap(Result::getRowsUpdated)
              .then(Mono.defer(() -> executeExpectedErrorStream("4. Constraint Violation Test", connection.createStatement("INSERT INTO dbo.ExceptionsTest (id) VALUES (1)").execute(), Result::getRowsUpdated)));
        }))

        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 5. Object Not Found Test ---");
          // Querying a table that does not exist
          return executeExpectedErrorStream("5. Object Not Found Test", connection.createStatement("SELECT * FROM dbo.TableDoesNotExistInDatabase").execute(), res -> res.map((r, m) -> r.get(0)));
        }))

        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 6. Exception Recovery Validation ---");
          // Ensure the connection is still perfectly usable after catching multiple severe SQL and downstream errors
          return Mono.from(connection.validate(ValidationDepth.REMOTE))
              .doOnNext(isValid -> {
                if (isValid) {
                  System.out.println("  -> SUCCESS: Connection recovered from exceptions and is healthy.");
                } else {
                  System.err.println("  -> FAILED: Connection died after throwing an exception.");
                }
              })
              .then();
        }))
        .contextWrite(Context.of("trace-id", traceId));
  }

  // --- Normal Execution Helper ---
  private <T> Mono<Void> executeStream(String stepName, Publisher<? extends Result> resultPublisher, Function<Result, Publisher<T>> extractor) {
    return Flux.from(resultPublisher)
        .flatMap(extractor)
        .doOnNext(item -> System.out.println("  -> " + item))
        .doOnError(error -> System.err.println("[" + stepName + "] ❌ Unexpected Failure: " + error.getMessage()))
        .then();
  }

  // --- Expected Failure Helper ---
  private <T> Mono<Void> executeExpectedErrorStream(String stepName, Publisher<? extends Result> resultPublisher, Function<Result, Publisher<T>> extractor) {
    return Flux.from(resultPublisher)
        .flatMap(extractor)
        .doOnNext(item -> System.out.println("  -> " + item))
        .then(Mono.<Void>error(new IllegalStateException("Step '" + stepName + "' was expected to throw an error, but it succeeded!")))
        .onErrorResume(e -> {
          if (e instanceof IllegalStateException) {
            return Mono.error(e);
          }
          // Print the class of the exception to verify it was mapped correctly
          System.out.println("  ✓ [Expected Error Caught]: " + e.getClass().getSimpleName() + " - " + e.getMessage());
          return Mono.empty();
        });
  }
}