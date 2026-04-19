package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.api.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsClientChaosTest {
  public static void main(String[] args) throws Exception {
    new TdsClientChaosTest().run();
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
    UUID traceId = UUID.randomUUID();
    System.out.println("Injecting Trace ID: " + traceId);

    // Chain the chaos tests sequentially on the same connection.
    // If the driver is truly stable, the connection will survive all of these
    // and process the final test perfectly.
    return test1_CancelMidStream(connection)
        .onErrorResume(e -> Mono.empty()) // Swallow expected errors to keep the suite running
        .then(test2_ThrowInUserCode(connection))
        .onErrorResume(e -> Mono.empty())
        .then(test3_SlowConsumer(connection))
        .then(test4_DelayedDemand(connection))
        .contextWrite(Context.of("trace-id", traceId));
  }

  // --- CHAOS TEST 1: The Mid-Stream Cancel ---
  private Mono<Void> test1_CancelMidStream(Connection connection) {
    System.out.println("\n--- CHAOS TEST 1: Mid-Stream Cancel (.take(2)) ---");
    return Flux.from(connection.createStatement(querySql).execute())
        .flatMap(res -> res.map(allDataTypesMapper))
        // .take(2) tells Reactor: "Read 2 rows, then instantly call cancel() on the driver"
        .take(2)
        .doOnNext(row -> System.out.println("  Read Row: " + row))
        .doOnCancel(() -> System.out.println("  [!] Reactor triggered CANCEL signal to driver."))
        .then();
  }

  // --- CHAOS TEST 2: The User Application Crash ---
  private Mono<Void> test2_ThrowInUserCode(Connection connection) {
    System.out.println("\n--- CHAOS TEST 2: Exception Thrown Mid-Stream ---");
    java.util.concurrent.atomic.AtomicInteger counter = new java.util.concurrent.atomic.AtomicInteger(0);

    return Flux.from(connection.createStatement(querySql).execute())
        .flatMap(res -> res.map(allDataTypesMapper))
        .doOnNext(row -> {
          System.out.println("  Read Row: " + row);
          if (counter.incrementAndGet() == 2) {
            System.out.println("  [!] User code throwing RuntimeException!");
            throw new RuntimeException("Simulated Application Crash!");
          }
        })
        .doOnError(e -> System.out.println("  Caught expected stream death: " + e.getMessage()))
        .then();
  }

  // --- CHAOS TEST 3: The Slow Backpressure Sink ---
  private Mono<Void> test3_SlowConsumer(Connection connection) {
    System.out.println("\n--- CHAOS TEST 3: Slow Consumer (High Watermark Test) ---");
    return Flux.from(connection.createStatement(querySql).execute())
        .flatMap(res -> res.map(allDataTypesMapper))
        // Force Reactor to wait 500ms before requesting the next item.
        // This will cause your TdsTokenQueue to buffer tokens and potentially hit the High Watermark.
        .delayElements(Duration.ofMillis(500))
        .doOnNext(row -> System.out.println("  Slowly Read Row: " + row))
        .then();
  }

  // --- CHAOS TEST 4: Zero Demand Deadlock Test ---
  private Mono<Void> test4_DelayedDemand(Connection connection) {
    System.out.println("\n--- CHAOS TEST 4: Delayed Demand (request(0) Test) ---");

    return Mono.create(sink -> {
      Flux.from(connection.createStatement(querySql).execute())
          .flatMap(res -> res.map(allDataTypesMapper))
          // FIXED: Using reactor.core.publisher.BaseSubscriber
          .subscribe(new reactor.core.publisher.BaseSubscriber<AllDataTypesRecord>() {
            @Override
            protected void hookOnSubscribe(org.reactivestreams.Subscription subscription) {
              System.out.println("  [!] Subscribed, but delaying request() for 2 seconds...");
              // We do NOT call request(n) immediately.
              // The driver's drain() loop must handle internal tokens while demand is 0.
              Mono.delay(Duration.ofSeconds(2)).subscribe(v -> {
                System.out.println("  [!] Requesting data now.");
                requestUnbounded();
              });
            }

            @Override
            protected void hookOnNext(AllDataTypesRecord value) {
              System.out.println("  Delayed Read Row: " + value);
            }

            @Override
            protected void hookOnComplete() {
              System.out.println("  Stream completed successfully.");
              sink.success();
            }

            @Override
            protected void hookOnError(Throwable throwable) {
              System.err.println("  Stream failed: " + throwable.getMessage());
              sink.error(throwable);
            }
          });
    });
  }

  // --- The Universal Async Helper (LATCH-FREE) ---

  private <T> Mono<Void> executeStream(String stepName, Publisher<? extends Result> resultPublisher, Function<Result, Publisher<T>> extractor) {
    System.out.println("\n--- Executing: " + stepName + " ---");

    // Build the reactive pipeline without subscribing or blocking
    return Flux.from(resultPublisher)
        .flatMap(extractor)
        .doOnNext(item -> System.out.println("  -> " + item))
        .doOnError(error -> System.err.println("[" + stepName + "] Stream Error: " + error.getMessage()))
        .doOnComplete(() -> System.out.println("--- Completed: " + stepName + " ---"))
        .then(); // .then() converts Flux<T> into a Mono<Void> that completes when the Flux finishes
  }

  // --- The Universal Async Helper using Reactor Flux ---

  BiFunction<Row, RowMetadata, AllDataTypesRecord> allDataTypesMapper = (row, meta) -> new AllDataTypesRecord(
      row.get(0, Integer.class),
      row.get(1, Boolean.class),
      row.get(2, Byte.class),
      row.get(3, Short.class),
      row.get(4, Integer.class),
      row.get(5, Long.class),
      row.get(6, BigDecimal.class),
      row.get(7, BigDecimal.class),
      row.get(8, BigDecimal.class),
      row.get(9, BigDecimal.class),
      row.get(10, Float.class),
      row.get(11, Double.class),
      row.get(12, LocalDate.class),
      row.get(13, LocalTime.class),
      row.get(14, LocalDateTime.class),
      row.get(15, LocalDateTime.class),
      row.get(16, LocalDateTime.class),
      row.get(17, OffsetDateTime.class),
      row.get(18, String.class),
      row.get(19, String.class),
      row.get(20, String.class),
      row.get(21, String.class),
      row.get(22, String.class),
      row.get(23, String.class),
      row.get(24, String.class),
      row.get(25, byte[].class),
      row.get(26, byte[].class),
      row.get(27, byte[].class),
      row.get(28, byte[].class),
      row.get(29, UUID.class),
      row.get(30, String.class)
  );

  private static final String querySql = """
    SET TEXTSIZE -1;
    SELECT * FROM dbo.AllDataTypes;
    """;
}