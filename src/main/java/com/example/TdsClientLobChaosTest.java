package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Clob;
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

public class TdsClientLobChaosTest {

  private static final Logger logger = LoggerFactory.getLogger(TdsClientLobChaosTest.class);

  // Generates a ~25MB string payload to ensure it is chunked over the network
  private static final String lobQuery = "SET TEXTSIZE -1; SELECT REPLICATE(CAST('A' AS VARCHAR(MAX)), 25000000) AS LargeString;";

  public static void main(String[] args) {
    new TdsClientLobChaosTest().run();
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

    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(connectionFactory)
        .initialSize(1).maxSize(1).build()); // Force single connection to prove no poisoning

    logger.info("Starting LOB Chaos Suite...");

    Mono.usingWhen(
            Mono.just(pool),
            this::runSql,
            p -> p.disposeLater().doOnSuccess(v -> logger.info("✅ LOB Chaos Suite Complete. Pool closed safely."))
        )
        .doOnError(t -> logger.error("❌ Fatal Suite Crash: {}", t.getMessage()))
        .block();
  }

  public Mono<Void> runSql(ConnectionPool pool) {
    return Mono.usingWhen(
        Mono.from(pool.create()),
        connection -> test1_CancelMidLob(connection)
            .onErrorResume(e -> Mono.empty())
            .then(test2_CrashMidLob(connection))
            .onErrorResume(e -> Mono.empty())
            .then(test3_SlowLobConsumer(connection))
            .then(test4_DelayedLobDemand(connection)),
        Connection::close
    );
  }

  // --- LOB CHAOS 1: The Mid-Stream Chunk Cancel ---
  private Mono<Void> test1_CancelMidLob(Connection connection) {
    logger.info("--- LOB CHAOS 1: Mid-LOB Cancel (.take(5) on Clob chunks) ---");
    return Flux.from(connection.createStatement(lobQuery).execute())
        .flatMap(res -> res.map((row, meta) -> row.get(0, Clob.class)))
        .flatMap(clob ->
            Flux.from(clob.stream())
                // Read 5 chunks of the 25MB string, then instantly abort!
                .take(5)
                .doOnNext(chunk -> logger.info("  Read Clob Chunk: {} chars", chunk.length()))
                .doOnCancel(() -> logger.info("  [!] CANCEL signaled mid-LOB stream!"))
        ).then();
  }

  // --- LOB CHAOS 2: The Application Crash Mid-LOB ---
  private Mono<Void> test2_CrashMidLob(Connection connection) {
    logger.info("--- LOB CHAOS 2: Exception Thrown Mid-LOB Chunk Processing ---");
    AtomicInteger chunkCounter = new AtomicInteger(0);

    return Flux.from(connection.createStatement(lobQuery).execute())
        .flatMap(res -> res.map((row, meta) -> row.get(0, Clob.class)))
        .flatMap(clob ->
            Flux.from(clob.stream())
                .doOnNext(chunk -> {
                  logger.info("  Read Clob Chunk: {} chars", chunk.length());
                  if (chunkCounter.incrementAndGet() == 3) {
                    logger.info("  [!] Throwing RuntimeException while draining LOB!");
                    throw new RuntimeException("Simulated LOB Application Crash!");
                  }
                })
        )
        .doOnError(e -> logger.info("  Caught expected LOB stream death: {}", e.getMessage()))
        .then();
  }

  // --- LOB CHAOS 3: The Slow LOB Consumer ---
  private Mono<Void> test3_SlowLobConsumer(Connection connection) {
    logger.info("--- LOB CHAOS 3: Slow LOB Consumer (Testing isPaused lock) ---");
    return Flux.from(connection.createStatement(lobQuery).execute())
        .flatMap(res -> res.map((row, meta) -> row.get(0, Clob.class)))
        .flatMap(clob ->
            Flux.from(clob.stream())
                // Delay each chunk by 100ms. This forces your activeRowDrainer
                // to pause the network via row.setAsyncCallbacks(pause)
                .delayElements(Duration.ofMillis(100))
                .doOnNext(chunk -> logger.info("  Slowly Read Clob Chunk: {} chars", chunk.length()))
        ).then();
  }

  // --- LOB CHAOS 4: Zero Demand LOB Start ---
  private Mono<Void> test4_DelayedLobDemand(Connection connection) {
    logger.info("--- LOB CHAOS 4: Delayed LOB Demand (request(0) Test) ---");

    return Mono.create(sink -> {
      Flux.from(connection.createStatement(lobQuery).execute())
          .flatMap(res -> res.map((row, meta) -> row.get(0, Clob.class)))
          .flatMap(clob -> Flux.from(clob.stream())) // Subscribe to the chunk stream
          .subscribe(new reactor.core.publisher.BaseSubscriber<CharSequence>() {
            @Override
            protected void hookOnSubscribe(org.reactivestreams.Subscription subscription) {
              logger.info("  [!] Subscribed to LOB chunks, but delaying request() for 2 seconds...");
              Mono.delay(Duration.ofSeconds(2)).subscribe(v -> {
                logger.info("  [!] Requesting LOB chunks now.");
                requestUnbounded();
              });
            }

            @Override
            protected void hookOnNext(CharSequence chunk) {
              logger.info("  Delayed Read Clob Chunk: {} chars", chunk.length());
            }

            @Override
            protected void hookOnComplete() {
              logger.info("  LOB Stream completed successfully.");
              sink.success();
            }

            @Override
            protected void hookOnError(Throwable throwable) {
              logger.error("  LOB Stream failed: {}", throwable.getMessage());
              sink.error(throwable);
            }
          });
    });
  }
}