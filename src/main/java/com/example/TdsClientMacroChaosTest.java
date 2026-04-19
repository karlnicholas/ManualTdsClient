package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.api.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsClientMacroChaosTest {

  private static final Logger logger = LoggerFactory.getLogger(TdsClientMacroChaosTest.class);

  // THE MACRO QUERY: Generates 100,000 rows to span multiple TCP packets
  private static final String querySql = """
    SET TEXTSIZE -1;
    SELECT TOP 100000 a.name 
    FROM sys.all_columns a 
    CROSS JOIN sys.all_columns b;
    """;

  public static void main(String[] args) {
    new TdsClientMacroChaosTest().run();
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

// CRITICAL: Force pool size to 1, AND add a validation query.
    // The pool will attempt to run "SELECT 1". When it fails on the poisoned
    // connection, the pool will automatically evict it and create a fresh one!
    ConnectionPoolConfiguration poolConfiguration = ConnectionPoolConfiguration.builder(connectionFactory)
        .initialSize(1)
        .maxSize(1)
        .maxIdleTime(Duration.ofMinutes(10))
        .validationQuery("SELECT 1") // <--- ADD THIS LINE
        .build();

    ConnectionPool pool = new ConnectionPool(poolConfiguration);

    logger.info("Starting MACRO Chaos Suite (100,000 Row Trap)...");

    Mono.usingWhen(
            Mono.just(pool),
            this::runSql,
            p -> p.disposeLater().doOnSuccess(v -> logger.info("✅ Tests complete. Connection pool closed."))
        )
        .doOnError(t -> logger.error("❌ Fatal Suite Crash: {}", t.getMessage()))
        .block();
  }

  public Mono<Void> runSql(ConnectionPool pool) {
    // Chain the tests, passing the POOL to each, not a single Connection.
    return test1_CancelMidStream(pool)
        .onErrorResume(e -> Mono.empty())
        .then(test2_ThrowInUserCode(pool))
        .onErrorResume(e -> Mono.empty())
        .then(test3_SlowConsumer(pool))
        .then(test4_DelayedDemand(pool));
  }

  // --- CHAOS TEST 1: The Mid-Stream Cancel ---
  private Mono<Void> test1_CancelMidStream(ConnectionPool pool) {
    logger.info("\n--- MACRO TEST 1: Mid-Stream Cancel (.take(2) out of 100,000) ---");
    return Mono.usingWhen(
        Mono.from(pool.create()),
        connection -> Flux.from(connection.createStatement(querySql).execute())
            .flatMap(res -> res.map((row, meta) -> row.get(0, String.class)))
            .take(2)
            .doOnNext(val -> logger.info("  Read Row: {}", val))
            .doOnCancel(() -> logger.info("  [!] CANCEL triggered. 99,998 rows left on the wire!"))
            .then(),
        Connection::close
    );
  }

  // --- CHAOS TEST 2: The User Application Crash ---
  private Mono<Void> test2_ThrowInUserCode(ConnectionPool pool) {
    logger.info("\n--- MACRO TEST 2: Exception Thrown Mid-Stream ---");
    AtomicInteger counter = new AtomicInteger(0);

    return Mono.usingWhen(
        Mono.from(pool.create()),
        connection -> Flux.from(connection.createStatement(querySql).execute())
            .flatMap(res -> res.map((row, meta) -> row.get(0, String.class)))
            .doOnNext(val -> {
              logger.info("  Read Row: {}", val);
              if (counter.incrementAndGet() == 2) {
                logger.info("  [!] User code throwing RuntimeException!");
                throw new RuntimeException("Simulated Application Crash!");
              }
            })
            .doOnError(e -> logger.info("  Caught expected stream death: {}", e.getMessage()))
            .then(),
        Connection::close
    );
  }

  // --- CHAOS TEST 3: The Slow Backpressure Sink ---
  private Mono<Void> test3_SlowConsumer(ConnectionPool pool) {
    logger.info("\n--- MACRO TEST 3: Slow Consumer (Testing High Watermark Suspend) ---");
    return Mono.usingWhen(
        Mono.from(pool.create()),
        connection -> Flux.from(connection.createStatement(querySql).execute())
            .flatMap(res -> res.map((row, meta) -> row.get(0, String.class)))
            .take(10) // Just take 10 to avoid waiting for thousands of delays
            .delayElements(Duration.ofMillis(500))
            .doOnNext(val -> logger.info("  Slowly Read Row: {}", val))
            .then(),
        Connection::close
    );
  }

  // --- CHAOS TEST 4: Zero Demand Deadlock Test ---
  private Mono<Void> test4_DelayedDemand(ConnectionPool pool) {
    logger.info("\n--- MACRO TEST 4: Delayed Demand (request(0) Test) ---");

    return Mono.usingWhen(
        Mono.from(pool.create()),
        connection -> Mono.create(sink -> {
          Flux.from(connection.createStatement(querySql).execute())
              .flatMap(res -> res.map((row, meta) -> row.get(0, String.class)))
              .take(5) // Just take 5 to prove it unstalls
              .subscribe(new reactor.core.publisher.BaseSubscriber<String>() {
                @Override
                protected void hookOnSubscribe(org.reactivestreams.Subscription subscription) {
                  logger.info("  [!] Subscribed, but delaying request() for 2 seconds...");
                  Mono.delay(Duration.ofSeconds(2)).subscribe(v -> {
                    logger.info("  [!] Requesting data now.");
                    requestUnbounded();
                  });
                }

                @Override
                protected void hookOnNext(String value) {
                  logger.info("  Delayed Read Row: {}", value);
                }

                @Override
                protected void hookOnComplete() {
                  logger.info("  Stream completed successfully.");
                  sink.success();
                }

                @Override
                protected void hookOnError(Throwable throwable) {
                  logger.error("  Stream failed: {}", throwable.getMessage());
                  sink.error(throwable);
                }
              });
        }),
        Connection::close
    ).then();
  }
}