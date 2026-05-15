package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.OutParameters;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.api.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsClientBindSpStress {

  public static void main(String[] args) {
    new TdsClientBindSpStress().run();
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
        .initialSize(10)
        .maxSize(10) // Fixed size for predictable stress testing
        .maxIdleTime(Duration.ofMinutes(10))
        .build();

    ConnectionPool pool = new ConnectionPool(poolConfiguration);

    System.out.println("Connecting to database pool for SP & Stress Testing...");

    Mono.usingWhen(
            Mono.just(pool),
            p -> runSql(p),
            p -> p.disposeLater().doOnSuccess(v -> System.out.println("\nConnection Pool safely closed."))
        )
        .doOnError(t -> System.err.println("\n❌ Test Suite Failed: " + t.getMessage()))
        .block();
  }

  public Mono<Void> runSql(ConnectionPool pool) {
    UUID traceId = UUID.randomUUID();
    return Mono.usingWhen(
        Mono.from(pool.create()),
        conn -> runSql(pool, conn, traceId), // Pass both pool and the main connection down
        Connection::close
    );
  }

  public Mono<Void> runSql(ConnectionPool pool, Connection connection, UUID traceId) {

    return Mono.defer(() -> executeStream("0. Setup Stored Procedure",
            connection.createStatement(createSpSql).execute(), Result::getRowsUpdated))

        // 1. Valid Stored Procedure with Result.flatMap iteration (Updated for R2DBC 1.0.0)
        .then(Mono.defer(() -> {
          Statement stmt = connection.createStatement("EXEC dbo.TestMultiResultProc @InputVal = @in, @OutputVal = @out OUTPUT")
              .bind("@in", 42)
              .bind("@out", Parameters.out(R2dbcType.INTEGER));

          return executeStream("1. Stored Proc (Multiple Results + OUT Params)", stmt.execute(), res -> res.flatMap(segment -> {
            if (segment instanceof Result.RowSegment rowSeg) {
              Row row = rowSeg.row();
              return Flux.just("ROW -> " + row.get("Label", String.class) + " | Value: " + row.get("DataValue"));
            } else if (segment instanceof Result.OutSegment outSeg) {
              OutParameters out = outSeg.outParameters();
              // Retrieve using the bound identifier "@out", or fallback to index 0
              return Flux.just("OUT PARAM -> @out: " + out.get("@out", Integer.class));
            } else if (segment instanceof Result.UpdateCount upd) {
              return Flux.just("UPDATE COUNT -> " + upd.value());
            } else if (segment instanceof Result.Message msg) {
              return Flux.just("MESSAGE -> " + msg.message());
            }
            return Flux.empty();
          }));
        }))

        // 2 & 3. Extensive Error Testing
        .then(Mono.defer(() -> executeExpectedErrorStream("2. SP Error (Missing Procedure)",
            connection.createStatement("EXEC dbo.ProcThatDoesNotExist").execute(), Result::getRowsUpdated)))

        .then(Mono.defer(() -> {
          Statement stmt = connection.createStatement("EXEC dbo.TestMultiResultProc @InputVal = 'InvalidString', @OutputVal = @out OUTPUT")
              .bind("@out", Parameters.out(R2dbcType.INTEGER));
          return executeExpectedErrorStream("3. SP Error (Invalid Parameter Type)", stmt.execute(), Result::getRowsUpdated);
        }))

        // 4. High Throughput Stress Test (Fixed Mono inference)
        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 4. High Throughput Stress Test (10k requests) ---");
          long start = System.currentTimeMillis();
          AtomicInteger counter = new AtomicInteger();

          return Flux.range(1, 10000)
              .flatMap(i -> Mono.usingWhen(
                  Mono.from(pool.create()), // Lease a new connection from the pool
                  c -> Mono.from(c.createStatement("SELECT " + i).execute())
                      .flatMapMany(res -> res.map((row, meta) -> row.get(0, Integer.class)))
                      .next(), // Resolves the generic inference error by returning a Mono
                  Connection::close
              ), 10) // Strict concurrency matches max pool size
              .doOnNext(v -> {
                if (counter.incrementAndGet() % 1000 == 0) System.out.print(".");
              })
              .then(Mono.fromRunnable(() -> {
                long duration = System.currentTimeMillis() - start;
                System.out.println("\n  -> Stress Test Completed: 10,000 queries in " + duration + "ms.");
                System.out.println("--- Completed: 4. High Throughput Stress Test ---");
              }));
        }))

        // 5. Final State Verification on the primary connection
        .then(Mono.defer(() -> executeStream("5. Final Driver Sanity Check",
            connection.createStatement("SELECT 'Driver state is synchronized and healthy!' AS Status").execute(),
            res -> res.map((row, meta) -> row.get("Status", String.class)))))

        .contextWrite(Context.of("trace-id", traceId));
  }

  // --- Strict Async Helper ---
  private <T> Mono<Void> executeStream(String stepName, Publisher<? extends Result> resultPublisher, Function<Result, Publisher<T>> extractor) {
    System.out.println("\n--- Executing: " + stepName + " ---");

    return Flux.from(resultPublisher)
        .flatMap(extractor)
        .doOnNext(item -> System.out.println("  -> " + item))
        .doOnError(error -> System.err.println("[" + stepName + "] ❌ FAILED: " + error.getMessage()))
        .doOnComplete(() -> System.out.println("--- Completed: " + stepName + " ---"))
        .then();
  }

  // --- Expected Failure Helper ---
  private <T> Mono<Void> executeExpectedErrorStream(String stepName, Publisher<? extends Result> resultPublisher, Function<Result, Publisher<T>> extractor) {
    System.out.println("\n--- Executing [EXPECTS ERROR]: " + stepName + " ---");

    return Flux.from(resultPublisher)
        .flatMap(extractor)
        .doOnNext(item -> System.out.println("  -> " + item))
        .then(Mono.<Void>error(new IllegalStateException("Step '" + stepName + "' was expected to throw an error, but it succeeded!")))
        .onErrorResume(e -> {
          if (e instanceof IllegalStateException) return Mono.error(e);
          System.out.println("  ✓ [Expected Database Error Caught]: " + e.getMessage());
          System.out.println("--- Completed: " + stepName + " ---");
          return Mono.empty();
        });
  }

  // --- SQL Definitions ---
  private static final String createSpSql = """
    CREATE OR ALTER PROCEDURE dbo.TestMultiResultProc
        @InputVal INT,
        @OutputVal INT OUTPUT
    AS
    BEGIN
        SET NOCOUNT ON;
        SET @OutputVal = @InputVal * 2;
        
        -- Result Set 1
        SELECT 'Multiplier Input' AS Label, @InputVal AS DataValue;
        
        -- Result Set 2
        SELECT 'Multiplier Output' AS Label, @OutputVal AS DataValue;
    END
    """;
}