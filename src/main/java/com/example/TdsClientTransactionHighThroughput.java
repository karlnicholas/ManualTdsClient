package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import org.tdslib.javatdslib.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class TdsClientTransactionHighThroughput {

  private static final int TRANSACTION_COUNT = 10000;
  private static final int CONCURRENCY_LIMIT = 50;

  public static void main(String[] args) {
    new TdsClientTransactionHighThroughput().run();
  }

  private void run() {
    ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(DRIVER, "javatdslib").option(HOST, "localhost").option(PORT, 1433)
        .option(PASSWORD, "reactnonreact").option(USER, "reactnonreact").option(DATABASE, "reactnonreact")
        .option(TdsLibOptions.TRUST_SERVER_CERTIFICATE, true).build());

    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(connectionFactory)
        .initialSize(10).maxSize(CONCURRENCY_LIMIT).maxIdleTime(Duration.ofMinutes(10)).build());

    System.out.println("Connecting to database pool for High Throughput Transaction Testing...");
    long startTime = System.currentTimeMillis();

    Mono.usingWhen(Mono.just(pool), this::runThroughputTest, p -> p.disposeLater())
        .doOnSuccess(v -> {
          long elapsed = System.currentTimeMillis() - startTime;
          System.out.println("\n✅ Transaction Throughput Test Complete.");
          System.out.println("  -> Executed " + TRANSACTION_COUNT + " full transactions (Begin -> Insert -> Commit) in " + elapsed + "ms.");
        })
        .doOnError(t -> System.err.println("\n❌ Test Suite Failed: " + t.getMessage()))
        .block();
  }

  public Mono<Void> runThroughputTest(ConnectionPool pool) {
    System.out.println("\n--- Executing: 10,000 Concurrent Transactions ---");

    Mono<Void> setup = Mono.usingWhen(Mono.from(pool.create()),
        conn -> Mono.from(conn.createStatement("DROP TABLE IF EXISTS dbo.HighThroughputTx; CREATE TABLE dbo.HighThroughputTx (id INT IDENTITY(1,1), val INT);").execute()).then(),
        Connection::close);

    // Blast the pool with discrete transactional boundaries
    Flux<Void> loadTest = Flux.range(1, TRANSACTION_COUNT)
        .flatMap(i ->
            Mono.usingWhen(Mono.from(pool.create()),
                conn -> executeFastTransaction(conn, i),
                Connection::close
            ), CONCURRENCY_LIMIT
        );

    return setup.thenMany(loadTest).then();
  }

  private Mono<Void> executeFastTransaction(Connection connection, int iteration) {
    return Mono.from(connection.beginTransaction())
        .thenMany(connection.createStatement("INSERT INTO dbo.HighThroughputTx (val) VALUES (@val)").bind("@val", iteration).execute())
        .flatMap(Result::getRowsUpdated)
        .then(Mono.from(connection.commitTransaction()))
        .onErrorResume(e -> {
          System.err.println("Transaction " + iteration + " failed: " + e.getMessage());
          // Attempt rollback if insertion failed
          return Mono.from(connection.rollbackTransaction()).onErrorResume(e2 -> Mono.empty());
        });
  }
}