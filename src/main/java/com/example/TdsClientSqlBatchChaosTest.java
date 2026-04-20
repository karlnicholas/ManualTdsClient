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

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class TdsClientSqlBatchChaosTest {

  private static final Logger logger = LoggerFactory.getLogger(TdsClientSqlBatchChaosTest.class);

  public static void main(String[] args) {
    new TdsClientSqlBatchChaosTest().run();
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
        .initialSize(2)
        .maxSize(10)
        .maxIdleTime(Duration.ofMinutes(10))
        .build();

    ConnectionPool pool = new ConnectionPool(poolConfiguration);

    logger.info("Connecting to pool for SQL Batch Chaos Suite...");

    Mono.usingWhen(
            Mono.just(pool),
            this::runSql,
            p -> p.disposeLater().doOnSuccess(v -> logger.info("\n✅ SQL Batch Chaos Suite Complete."))
        )
        .doOnError(t -> logger.error("\n❌ Test Suite Failed: {}", t.getMessage()))
        .block();
  }

  public Mono<Void> runSql(ConnectionPool pool) {
    return Mono.usingWhen(
        Mono.from(pool.create()),
        connection -> test1_HappyPathMultiStatement(connection)
            .then(test2_TheMultiStatementTrap(connection))
            .then(test3_TheVictim(connection)),
        Connection::close
    );
  }

  // ---------------------------------------------------------------------------------------
  // TEST 1: The Happy Path (Runs to completion)
  // ---------------------------------------------------------------------------------------
  private Mono<Void> test1_HappyPathMultiStatement(Connection connection) {
    logger.info("\n--- TEST 1: Happy Path Multi-Statement (3 Queries) ---");

    // A single string containing 3 completely different statements
    String rawSql = "SELECT 'Query A' AS val; SELECT 'Query B' AS val; SELECT 'Query C' AS val;";

    return Flux.from(connection.createStatement(rawSql).execute())
        .flatMap(result -> result.map((row, meta) -> row.get(0, String.class)))
        .doOnNext(val -> logger.info("  Happy Path Result: {}", val))
        .then();
  }

  // ---------------------------------------------------------------------------------------
  // TEST 2: The Trap (Massive Raw SQL Batch + .take(2))
  // ---------------------------------------------------------------------------------------
  private Mono<Void> test2_TheMultiStatementTrap(Connection connection) {
    logger.info("\n--- TEST 2: The Trap (Massive SQL Batch + .take(2)) ---");

    // Build a massive, multi-statement string.
    // 2,000 distinct SELECT statements sent in a single physical payload.
    StringBuilder massiveSqlBatch = new StringBuilder();
    for (int i = 1; i <= 2000; i++) {
      massiveSqlBatch.append("SELECT 'Chaos_Iteration_").append(i).append("' AS chaos_val; ");
    }

    return Flux.from(connection.createStatement(massiveSqlBatch.toString()).execute())
        .flatMap(result -> result.map((row, meta) -> row.get(0, String.class)))
        .take(2) // We pull the first 2 results, and then Reactor fires cancel()
        .doOnNext(val -> logger.info("  Trap read value: {}", val))
        .doOnCancel(() -> logger.warn("  [!] Trap Cancelled! SQL Server is still streaming 1,998 result sets!"))
        .then();
  }

  // ---------------------------------------------------------------------------------------
  // TEST 3: The Victim
  // ---------------------------------------------------------------------------------------
  private Mono<Void> test3_TheVictim(Connection connection) {
    logger.info("\n--- TEST 3: The Victim (Executing on poisoned socket) ---");

    // This query will attempt to grab the lock while the 1,998 leftover RowTokens
    // and DONE tokens from Test 2 are crossing the physical TCP boundary.
    return Mono.from(connection.createStatement("SELECT 'I survived!' AS alive").execute())
        .flatMapMany(result -> result.map((row, meta) -> row.get(0, String.class)))
        .doOnNext(val -> logger.info("  Victim survived with value: {}", val))
        .then();
  }
}