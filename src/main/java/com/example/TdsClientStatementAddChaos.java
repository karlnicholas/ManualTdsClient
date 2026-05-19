package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class TdsClientStatementAddChaos {
  private static final Logger logger = LoggerFactory.getLogger(TdsClientStatementAddChaos.class);

  public static void main(String[] args) {
    new TdsClientStatementAddChaos().run();
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

    // Pool needs to be big enough for Test 3 (Concurrency 20) and Test 5 (Swarm)
    ConnectionPoolConfiguration poolConfiguration = ConnectionPoolConfiguration.builder(connectionFactory)
        .initialSize(20)
        .maxSize(30)
        .maxIdleTime(Duration.ofMinutes(10))
        .build();

    ConnectionPool pool = new ConnectionPool(poolConfiguration);

    logger.info("Connecting to pool for StatementAdd Chaos Suite...");

    Mono.usingWhen(
            Mono.just(pool),
            this::runSql,
            p -> p.disposeLater().doOnSuccess(v -> logger.info("\n✅ StatementAdd Chaos Suite Complete. Connection pool closed."))
        )
        .doOnError(t -> logger.error("\n❌ Test Suite Failed: {}", t.getMessage()))
        .block();
  }

  public Mono<Void> runSql(ConnectionPool pool) {
    // 1. Setup runs sequentially first to ensure the table exists
    return setupTable(pool)
        // 2. All Chaos tests are fired simultaneously to fight for pool connections
        .then(Mono.defer(() -> {
          logger.info("\n🔥 TRIGGERING INTERNAL TESTS CONCURRENTLY...");
          return Mono.when(
              test1_SimpleBatch(pool),
              test2_StressBatchSingleConn(pool),
              test3_StressBatchConcurrent(pool),
              test4_TheTrailingTokenTrap(pool),
              test5_TheVictimSwarm(pool),
              testDirectCrash(pool),
              testParserCrash(pool)
          );
        }));
  }

  private Mono<Void> setupTable(ConnectionPool pool) {
    return Mono.usingWhen(
        Mono.from(pool.create()),
        connection -> {
          String ddl = "DROP TABLE IF EXISTS dbo.StatementAdd; " +
              "CREATE TABLE dbo.StatementAdd (" +
              "id INT IDENTITY(1,1) PRIMARY KEY, " +
              "statement_add_name VARCHAR(50), " +
              "iteration INT" +
              ");";
          return Mono.from(connection.createStatement(ddl).execute()).then();
        },
        Connection::close
    );
  }

  // ---------------------------------------------------------------------------------------
  // TEST 1: Simple 3 StatementAdd Test
  // ---------------------------------------------------------------------------------------
  private Mono<Void> test1_SimpleBatch(ConnectionPool pool) {
    return Mono.usingWhen(
        Mono.from(pool.create()),
        connection -> {
          logger.info("--- TEST 1: Simple 3 StatementAdd Test Started ---");
          Statement stmt = connection.createStatement(
              "INSERT INTO dbo.StatementAdd (statement_add_name, iteration) VALUES (@name, @iter)");

          // Statements are now strictly thread-confined to this connection scope
          stmt.bind("name", "SimpleStatementAdd").bind("iter", 1).add()
              .bind("name", "SimpleStatementAdd").bind("iter", 2).add()
              .bind("name", "SimpleStatementAdd").bind("iter", 3);

          return Flux.from(stmt.execute())
              .flatMap(Result::getRowsUpdated)
              .doOnNext(rows -> logger.info("  SimpleStatementAdd row updated: {}", rows))
              .then();
        },
        Connection::close
    );
  }

  // ---------------------------------------------------------------------------------------
  // TEST 2: Stressful StatementAdd Test (1000 items)
  // ---------------------------------------------------------------------------------------
  private Mono<Void> test2_StressBatchSingleConn(ConnectionPool pool) {
    return Mono.usingWhen(
        Mono.from(pool.create()),
        connection -> {
          logger.info("--- TEST 2: Stress StatementAdd (1000 items) Started ---");
          Statement stmt = connection.createStatement(
              "INSERT INTO dbo.StatementAdd (statement_add_name, iteration) VALUES (@name, @iter)");

          for (int i = 1; i <= 1000; i++) {
            stmt.bind("name", "StressSingle").bind("iter", i);
            if (i < 1000) {
              stmt.add();
            }
          }

          return Flux.from(stmt.execute())
              .flatMap(Result::getRowsUpdated)
              .count()
              .doOnNext(count -> logger.info("  StressSingle processed {} StatementAdd results.", count))
              .then();
        },
        Connection::close
    );
  }

  // ---------------------------------------------------------------------------------------
  // TEST 3: Stressful StatementAdd Test (Connection Pool, Concurrency Level 20)
  // ---------------------------------------------------------------------------------------
  private Mono<Void> test3_StressBatchConcurrent(ConnectionPool pool) {
    logger.info("--- TEST 3: Stress StatementAdd (Pool Concurrency Level 20) Started ---");

    return Flux.range(1, 20)
        .flatMap(workerId -> Mono.usingWhen(
            Mono.from(pool.create()),
            conn -> {
              Statement stmt = conn.createStatement(
                  "INSERT INTO dbo.StatementAdd (statement_add_name, iteration) VALUES (@name, @iter)");
              for (int i = 1; i <= 50; i++) {
                stmt.bind("name", "ConcurrentWorker_" + workerId).bind("iter", i);
                if (i < 50) stmt.add();
              }
              return Flux.from(stmt.execute())
                  .flatMap(Result::getRowsUpdated)
                  .then();
            },
            Connection::close
        ), 20)
        .doOnComplete(() -> logger.info("  All 20 concurrent workers finished successfully."))
        .then();
  }

  // ---------------------------------------------------------------------------------------
  // TEST 4: The Trailing Token Trap (Intentional Mid-StatementAdd Cancellation)
  // ---------------------------------------------------------------------------------------
  private Mono<Void> test4_TheTrailingTokenTrap(ConnectionPool pool) {
    return Mono.usingWhen(
        Mono.from(pool.create()),
        connection -> {
          logger.info("--- TEST 4: The Trailing Token Trap Started ---");
          Statement stmt = connection.createStatement(
              "INSERT INTO dbo.StatementAdd (statement_add_name, iteration) VALUES (@name, @iter)");

          int massiveBatchSize = 2000;
          for (int i = 1; i <= massiveBatchSize; i++) {
            stmt.bind("name", "TheTrap").bind("iter", i);
            if (i < massiveBatchSize) stmt.add();
          }

          return Flux.from(stmt.execute())
              .take(2)
              .flatMap(Result::getRowsUpdated)
              .doOnNext(rows -> logger.info("  Trap read row updated: {}", rows))
              .doOnCancel(() -> logger.warn("  [!] Trap Cancelled! Releasing dirty connection back to pool..."))
              .then();
        },
        Connection::close
    );
  }

  // ---------------------------------------------------------------------------------------
  // TEST 5: The Victim Swarm
  // ---------------------------------------------------------------------------------------
  private Mono<Void> test5_TheVictimSwarm(ConnectionPool pool) {
    logger.info("--- TEST 5: The Victim Swarm (Hunting for poisoned sockets) Started ---");

    // Borrows 50 connections rapidly to try and snag the dirty socket released by Test 4.
    // If Test 4 properly holds the Attention lock, this swarm will pass unharmed.
    return Flux.range(1, 50)
        .flatMap(i -> Mono.delay(Duration.ofMillis(5)) // Slight stagger to hit the exact return window
            .then(Mono.usingWhen(
                Mono.from(pool.create()),
                conn -> Mono.from(conn.createStatement("SELECT 1 AS alive").execute())
                    .flatMapMany(result -> result.map((row, meta) -> row.get("alive", Integer.class)))
                    .then(),
//                // The Clean Execution
//                conn -> Flux.from(conn.createStatement("SELECT 1 AS alive").execute())
//                    .flatMap(result -> result.map((row, meta) -> row.get("alive", Integer.class)))
//                    .then(),
                Connection::close
            ))
        )
        .then()
        .doOnSuccess(v -> logger.info("  Victim Swarm survived with 0 casualties. Pool isolation is sound."));
  }

  // ---------------------------------------------------------------------------------------
  // TEST: The Direct Crash (No Pool, No Concurrency)
  // ---------------------------------------------------------------------------------------
  private Mono<Void> testDirectCrash(ConnectionFactory connectionFactory) {
    System.out.println("\n--- TEST: The Direct Crash Started ---");

    // 1. Open exactly ONE connection directly (bypassing the pool entirely)
    return Mono.from(connectionFactory.create())
        .flatMap(conn -> {

          System.out.println("  -> Step 1: Running massive query and forcing an early cancel...");

          // Toned down: 100,000 rows is ~3MB of data. Enough to force mid-stream fragmentation,
          // but small enough to trace cleanly.
          return Mono.from(conn.createStatement(
                  "SELECT TOP 100000 a.name FROM master.dbo.spt_values a CROSS JOIN master.dbo.spt_values b"
              ).execute())
              .flatMapMany(result -> result.map((row, meta) -> {
                String val = row.get(0, String.class);
                return val == null ? "DB_NULL" : val; // Guard against Reactor NPE
              }))
              // Grab the first 100 rows just to ensure the pipeline is heavily saturated
              .take(100)
              .count()
              .doOnNext(c -> System.out.println("  -> Grabbed " + c + " rows, firing cancel NOW..."))

              // 2. Wait 50ms for the server to send the DONE_ATTN bytes back
              .then(Mono.delay(Duration.ofMillis(50)))

              // 3. Run a second query on the EXACT SAME connection object
              .then(Mono.defer(() -> {
                System.out.println("  -> Step 2: Running SELECT 2 on the same connection...");
                return Flux.from(conn.createStatement("SELECT 2 AS alive").execute())
                    .flatMap(result -> result.map((row, meta) -> row.get("alive", Integer.class)))
                    .doOnNext(alive -> System.out.println("  -> Step 2 SUCCESS! Server returned: " + alive))
                    .then();
              }))

              // 4. Clean up
              .doFinally(signal -> Mono.from(conn.close()).subscribe());
        })
        .doOnError(e -> System.err.println("💥 CRASHED EXACTLY AS EXPECTED: " + e.getMessage()))
        .then();
  }

  // ---------------------------------------------------------------------------------------
  // TEST: The Parser Crash (No Cancel, Full Consumption)
  // ---------------------------------------------------------------------------------------
  private Mono<Void> testParserCrash(ConnectionFactory connectionFactory) {
    System.out.println("\n--- TEST: The Parser Crash Started ---");

    return Mono.from(connectionFactory.create())
        .flatMap(conn -> {

          System.out.println("  -> Step 1: Running massive query and consuming ALL rows...");
          return Mono.from(conn.createStatement("SELECT * FROM master.dbo.spt_values").execute())
              .flatMapMany(result -> result.map((row, meta) -> {
                Object val = row.get(0, Object.class);
                return val == null ? "DB_NULL" : val;
              }))

              // THE CHANGE: Consume every row to completion instead of cancelling
              .count()
              .doOnNext(count -> System.out.println("  -> Successfully parsed " + count + " rows."))

              .then(Mono.defer(() -> {
                System.out.println("  -> Step 2: Running SELECT 2 on the same connection...");
                return Flux.from(conn.createStatement("SELECT 2 AS alive").execute())
                    .flatMap(result -> result.map((row, meta) -> row.get("alive", Integer.class)))
                    .then();
              }))
              .doFinally(signal -> Mono.from(conn.close()).subscribe());
        })
        .doOnError(e -> System.err.println("💥 CRASHED EXACTLY AS EXPECTED: " + e.getMessage()))
        .then();
  }
}