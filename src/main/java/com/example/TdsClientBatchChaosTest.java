package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.api.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class TdsClientBatchChaosTest {
  private static final Logger logger = LoggerFactory.getLogger(TdsClientBatchChaosTest.class);

  public static void main(String[] args) {
    new TdsClientBatchChaosTest().run();
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

    // Pool needs to be big enough for Test 3 (Concurrency 20)
    ConnectionPoolConfiguration poolConfiguration = ConnectionPoolConfiguration.builder(connectionFactory)
        .initialSize(20)
        .maxSize(20)
        .maxIdleTime(Duration.ofMinutes(10))
        .build();

    ConnectionPool pool = new ConnectionPool(poolConfiguration);

    logger.info("Connecting to pool for Batch Chaos Suite...");

    Mono.usingWhen(
            Mono.just(pool),
            this::runSql,
            p -> p.disposeLater().doOnSuccess(v -> logger.info("\n✅ Batch Chaos Suite Complete. Connection pool closed."))
        )
        .doOnError(t -> logger.error("\n❌ Test Suite Failed: {}", t.getMessage()))
        .block();
  }

  public Mono<Void> runSql(ConnectionPool pool) {
    // Note: We use a single connection for setup, Test 1, Test 2, and Test 4.
    // Test 3 handles its own concurrent connection borrowing.
    return Mono.usingWhen(
        Mono.from(pool.create()),
        connection -> setupTable(connection)
            .then(test1_SimpleBatch(connection))
            .then(test2_StressBatchSingleConn(connection))
            .then(test3_StressBatchConcurrent(pool)) // Passes the pool for concurrent execution
            .then(test4_TheTrailingTokenTrap(connection)) // The 2000-batch trap
            .then(test5_TheVictim(connection)),           // The query that steps in it
        Connection::close
    );
  }

  private Mono<Void> setupTable(Connection connection) {
    String ddl = "DROP TABLE IF EXISTS dbo.BatchStress; " +
        "CREATE TABLE dbo.BatchStress (" +
        "id INT IDENTITY(1,1) PRIMARY KEY, " +
        "batch_name VARCHAR(50), " +
        "iteration INT" +
        ");";
    return Mono.from(connection.createStatement(ddl).execute()).then();
  }

  // ---------------------------------------------------------------------------------------
  // TEST 1: Simple 3 Batch Test
  // ---------------------------------------------------------------------------------------
  private Mono<Void> test1_SimpleBatch(Connection connection) {
    logger.info("\n--- TEST 1: Simple 3 Batch Test ---");
    Statement stmt = connection.createStatement(
        "INSERT INTO dbo.BatchStress (batch_name, iteration) VALUES (@name, @iter)");

    stmt.bind("name", "SimpleBatch").bind("iter", 1).add()
        .bind("name", "SimpleBatch").bind("iter", 2).add()
        .bind("name", "SimpleBatch").bind("iter", 3); // Final parameter set

    return Flux.from(stmt.execute())
        .flatMap(Result::getRowsUpdated)
        .doOnNext(rows -> logger.info("  SimpleBatch row updated: {}", rows))
        .then();
  }

  // ---------------------------------------------------------------------------------------
  // TEST 2: Stressful Batch Test (Single Connection, 1000 items)
  // ---------------------------------------------------------------------------------------
  private Mono<Void> test2_StressBatchSingleConn(Connection connection) {
    logger.info("\n--- TEST 2: Stress Batch (Single Connection, 1000 items) ---");
    Statement stmt = connection.createStatement(
        "INSERT INTO dbo.BatchStress (batch_name, iteration) VALUES (@name, @iter)");

    for (int i = 1; i <= 1000; i++) {
      stmt.bind("name", "StressSingle").bind("iter", i);
      if (i < 1000) {
        stmt.add(); // Save all except the last one
      }
    }

    // Use count() to simply wait for all 1000 results to process without blowing up the console
    return Flux.from(stmt.execute())
        .flatMap(Result::getRowsUpdated)
        .count()
        .doOnNext(count -> logger.info("  StressSingle processed {} batch results.", count))
        .then();
  }

  // ---------------------------------------------------------------------------------------
  // TEST 3: Stressful Batch Test (Connection Pool, Concurrency Level 20)
  // ---------------------------------------------------------------------------------------
  private Mono<Void> test3_StressBatchConcurrent(ConnectionPool pool) {
    logger.info("\n--- TEST 3: Stress Batch (Pool Concurrency Level 20) ---");

    // Creates 20 parallel reactive pipelines. Each one borrows its own connection
    // from the pool and executes a batch of 50 inserts.
    return Flux.range(1, 20)
        .flatMap(workerId -> Mono.usingWhen(
            Mono.from(pool.create()),
            conn -> {
              Statement stmt = conn.createStatement(
                  "INSERT INTO dbo.BatchStress (batch_name, iteration) VALUES (@name, @iter)");
              for (int i = 1; i <= 50; i++) {
                stmt.bind("name", "ConcurrentWorker_" + workerId).bind("iter", i);
                if (i < 50) stmt.add();
              }
              return Flux.from(stmt.execute())
                  .flatMap(Result::getRowsUpdated)
                  .then();
            },
            Connection::close
        ), 20) // The '20' here enforces the exact concurrency level in flatMap
        .doOnComplete(() -> logger.info("  All 20 concurrent workers finished successfully."))
        .then();
  }

  // ---------------------------------------------------------------------------------------
  // TEST 4: The Trailing Token Trap (Intentional Mid-Batch Cancellation)
  // ---------------------------------------------------------------------------------------
  private Mono<Void> test4_TheTrailingTokenTrap(Connection connection) {
    logger.info("\n--- TEST 4: The Trailing Token Trap (Massive Batch + .take(2)) ---");
    Statement stmt = connection.createStatement(
        "INSERT INTO dbo.BatchStress (batch_name, iteration) VALUES (@name, @iter)");

    // Batch of 2000 to completely shatter the 8000-byte TDS packet boundary
    int massiveBatchSize = 2000;
    for (int i = 1; i <= massiveBatchSize; i++) {
      stmt.bind("name", "TheTrap").bind("iter", i);
      if (i < massiveBatchSize) {
        stmt.add();
      }
    }

    return Flux.from(stmt.execute())
        .take(2)
        .flatMap(Result::getRowsUpdated)
        .doOnNext(rows -> logger.info("  Trap read row updated: {}", rows))
        .doOnCancel(() -> logger.warn("  [!] Trap Cancelled! Remaining batch tokens are flying across the wire!"))
        .then();
  }

  // ---------------------------------------------------------------------------------------
  // TEST 5: The Victim
  // ---------------------------------------------------------------------------------------
  private Mono<Void> test5_TheVictim(Connection connection) {
    logger.info("\n--- TEST 5: The Victim (Executing on poisoned socket) ---");

    // This query will attempt to run while the 1,998 leftover responses from Test 4 are still arriving.
    // Without a Graceful Discard state holding the lock, this will instantly Protocol Desync.
    return Mono.from(connection.createStatement("SELECT 1 AS alive").execute())
        .flatMapMany(result -> result.map((row, meta) -> row.get("alive", Integer.class)))
        .doOnNext(val -> logger.info("  Victim survived with value: {}", val))
        .then();
  }
}