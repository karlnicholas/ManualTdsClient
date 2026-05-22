package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.ValidationDepth;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Function;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsClientBatch {

  public static void main(String[] args) {
    new TdsClientBatch().run();
  }

  private void run() {
    String r2dbcUrl = "r2dbc:mssql://reactnonreact:reactnonreact@localhost:1433/reactnonreact?trustServerCertificate=true";

    // 2. Pass it directly to the factory
    ConnectionFactory connectionFactory = ConnectionFactories.get(r2dbcUrl);

    ConnectionPoolConfiguration poolConfiguration = ConnectionPoolConfiguration.builder(connectionFactory)
        .initialSize(2)
        .maxSize(10)
        .maxIdleTime(Duration.ofMinutes(10))
        .build();

    ConnectionPool pool = new ConnectionPool(poolConfiguration);

    System.out.println("Connecting to database pool for Batch Testing...");

    UUID traceId = UUID.randomUUID();

    Mono.usingWhen(
            Mono.just(pool),
            p -> runSql(p, traceId),
            p -> p.disposeLater().doOnSuccess(v -> System.out.println("\nConnection Pool safely closed."))
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

  @SuppressWarnings("JpaQueryApiInspection")
  public Mono<Void> runSql(Connection connection, UUID traceId) {

    return Mono.defer(() -> {
          System.out.println("\n--- Executing: 1. Setup DDL Batch ---");
          Batch setupBatch = connection.createBatch();
          setupBatch.add("DROP TABLE IF EXISTS dbo.StaticBatchTest");
          setupBatch.add("CREATE TABLE dbo.StaticBatchTest (id INT IDENTITY(1,1), event_name VARCHAR(50), active BIT)");
          return executeStream("1. Setup DDL Batch", setupBatch.execute(), Result::getRowsUpdated);
        })

        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 2. Simple Multi-Insert Batch ---");
          Batch insertBatch = connection.createBatch();
          insertBatch.add("INSERT INTO dbo.StaticBatchTest (event_name, active) VALUES ('Startup', 1)");
          insertBatch.add("INSERT INTO dbo.StaticBatchTest (event_name, active) VALUES ('Processing', 1)");
          insertBatch.add("INSERT INTO dbo.StaticBatchTest (event_name, active) VALUES ('Shutdown', 0)");
          return executeStream("2. Simple Multi-Insert Batch", insertBatch.execute(), Result::getRowsUpdated);
        }))

        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 3. Mixed DML & DQL Batch ---");
          // This tests a batch that modifies data and then immediately queries it back.
          // Requires parsing both UpdateCounts and RowSegments from the SPI Result stream.
          Batch mixedBatch = connection.createBatch();
          mixedBatch.add("UPDATE dbo.StaticBatchTest SET event_name = 'Processing_Complete' WHERE id = 2");
          mixedBatch.add("SELECT id, event_name, active FROM dbo.StaticBatchTest ORDER BY id ASC");

          return executeStream("3. Mixed DML & DQL Batch", mixedBatch.execute(), res ->
              res.flatMap(segment -> {
                if (segment instanceof Result.UpdateCount updateCount) {
                  return Mono.just("Update Count Segment: " + updateCount.value() + " row(s) affected.");
                } else if (segment instanceof Result.RowSegment rowSegment) {
                  Row row = rowSegment.row();
                  return Mono.just(String.format("Row Segment: ID=%d, Event=%s, Active=%b",
                      row.get("id", Integer.class),
                      row.get("event_name", String.class),
                      row.get("active", Boolean.class)));
                }
                return Mono.empty();
              })
          );
        }))

        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 4. Multiple Selects Batch ---");
          // A batch returning multiple result sets sequentially.
          Batch multiSelectBatch = connection.createBatch();
          multiSelectBatch.add("SELECT COUNT(*) AS total FROM dbo.StaticBatchTest");
          multiSelectBatch.add("SELECT event_name FROM dbo.StaticBatchTest WHERE active = 0");

          // Using a simple map since we only care about row output here
          return executeStream("4. Multiple Selects Batch", multiSelectBatch.execute(), res ->
              res.map((row, meta) -> "Row result from multi-select: " + row.get(0, Object.class))
          );
        }))

         .then(Mono.defer(() -> {
           System.out.println("\n--- Executing: 5. Empty Batch Test ---");
           Batch emptyBatch = connection.createBatch();
           // Deliberately not adding any statements
           return executeStream("5. Empty Batch Test", emptyBatch.execute(), Result::getRowsUpdated);
         }))

        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 6. Partial Failure Batch Test ---");
          // Tests how the driver handles a batch where one statement contains bad SQL.
          // TDS behavior typically aborts the batch at the failure point, throwing an exception.
          Batch errorBatch = connection.createBatch();
          errorBatch.add("INSERT INTO dbo.StaticBatchTest (event_name, active) VALUES ('Valid_1', 1)");
          errorBatch.add("INSERT INTO dbo.StaticBatchTest (invalid_column, active) VALUES ('Bad_Data', 1)"); // Expected to fail
          errorBatch.add("INSERT INTO dbo.StaticBatchTest (event_name, active) VALUES ('Valid_2', 1)");

          return executeExpectedErrorStream("6. Partial Failure Batch Test", errorBatch.execute(), Result::getRowsUpdated);
        }))

        // --- Update 1: Step 7 in runSql ---
        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 7. Connection Validation ---");
          // Use Flux.from() to prevent Mono.from() from sending an aggressive cancel signal
          return Flux.from(connection.validate(ValidationDepth.REMOTE))
              .doOnNext(isValid -> {
                if (isValid) {
                  System.out.println("  -> SUCCESS: Connection is healthy and ready for the pool.");
                } else {
                  System.err.println("  -> FAILED: Connection is dead or out of sync.");
                }
              })
              .doOnComplete(() -> System.out.println("--- Completed: 7. Connection Validation ---"))
              .then();
        }))
        // Inside runSql, add this right after Step 7
        .then(Mono.delay(Duration.ofMillis(50)).then())
        .contextWrite(Context.of("trace-id", traceId));
  }

  // --- Strict Async Helper ---
  private <T> Mono<Void> executeStream(String stepName, Publisher<? extends Result> resultPublisher, Function<Result, Publisher<T>> extractor) {
    return Flux.from(resultPublisher)
        .flatMap(extractor)
        .doOnNext(item -> System.out.println("  -> " + item))
        .doOnError(error -> System.err.println("[" + stepName + "] ❌ FAILED: " + error.getMessage()))
        .doOnComplete(() -> System.out.println("--- Completed: " + stepName + " ---"))
        .then();
  }

  // --- Update 2: The Helper Method ---
  private <T> Mono<Void> executeExpectedErrorStream(String stepName, Publisher<? extends Result> resultPublisher, Function<Result, Publisher<T>> extractor) {
    java.util.concurrent.atomic.AtomicBoolean errorCaught = new java.util.concurrent.atomic.AtomicBoolean(false);

    return Flux.from(resultPublisher)
        .flatMap(result -> Flux.from(extractor.apply(result))
            // Catch the error INNER stream so the OUTER resultPublisher is not cancelled
            .onErrorResume(e -> {
              System.out.println("  ✓ [Expected Database Error Caught]: " + e.getMessage());
              errorCaught.set(true);
              return Mono.empty();
            })
        )
        .doOnNext(item -> System.out.println("  -> " + item))
        .doOnComplete(() -> System.out.println("--- Completed: " + stepName + " ---"))
        .then(Mono.defer(() -> {
          // Evaluate our expectation safely after the stream naturally completes
          if (!errorCaught.get()) {
            return Mono.error(new IllegalStateException("Step '" + stepName + "' was expected to throw an error, but it succeeded!"));
          }
          return Mono.empty();
        }));
  }
}