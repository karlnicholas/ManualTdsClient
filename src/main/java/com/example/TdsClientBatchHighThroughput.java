package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import org.tdslib.r2dbc.mssql.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

import java.time.Duration;
import java.util.UUID;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsClientBatchHighThroughput {

  private static final int REQUEST_COUNT = 10000;
  private static final int CONCURRENCY_LIMIT = 50;

  public static void main(String[] args) {
    new TdsClientBatchHighThroughput().run();
  }

  private void run() {
    ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(ConnectionFactoryOptions.DRIVER, "mssql")
        .option(HOST, "localhost")
        .option(PORT, 1433)
        .option(PASSWORD, "reactnonreact")
        .option(USER, "reactnonreact")
        .option(DATABASE, "reactnonreact")
        .option(TdsLibOptions.TRUST_SERVER_CERTIFICATE, true)
        .build());

    // Larger pool to handle high concurrency
    ConnectionPoolConfiguration poolConfiguration = ConnectionPoolConfiguration.builder(connectionFactory)
        .initialSize(10)
        .maxSize(CONCURRENCY_LIMIT)
        .maxIdleTime(Duration.ofMinutes(10))
        .build();

    ConnectionPool pool = new ConnectionPool(poolConfiguration);

    System.out.println("Connecting to database pool for High Throughput Testing...");

    UUID traceId = UUID.randomUUID();
    long startTime = System.currentTimeMillis();

    Mono.usingWhen(
            Mono.just(pool),
            p -> runThroughputTest(p, traceId),
            p -> p.disposeLater().doOnSuccess(v -> System.out.println("\nConnection Pool safely closed."))
        )
        .doOnSuccess(v -> {
          long elapsed = System.currentTimeMillis() - startTime;
          System.out.println("\n✅ Throughput Test Complete.");
          System.out.println("  -> Executed " + REQUEST_COUNT + " requests in " + elapsed + "ms.");
        })
        .doOnError(t -> System.err.println("\n❌ Test Suite Failed: " + t.getMessage()))
        .block();
  }

  public Mono<Void> runThroughputTest(ConnectionPool pool, UUID traceId) {
    System.out.println("\n--- Executing: 10,000 Concurrent Requests ---");

    // Setup a basic table first
    Mono<Void> setup = Mono.usingWhen(
        Mono.from(pool.create()),
        conn -> Mono.from(conn.createStatement("DROP TABLE IF EXISTS dbo.HighThroughput; CREATE TABLE dbo.HighThroughput (id INT IDENTITY(1,1), val INT);").execute()).then(),
        Connection::close
    );

    // Blast the pool with requests.
    // flatMap with concurrency limit ensures we don't overwhelm Reactor, but pushes the pool to its max.
    Flux<Void> loadTest = Flux.range(1, REQUEST_COUNT)
        .flatMap(i ->
            Mono.usingWhen(
                Mono.from(pool.create()),
                conn -> executeFastQuery(conn, i),
                Connection::close
            ), CONCURRENCY_LIMIT
        );

    return setup.thenMany(loadTest).then().contextWrite(Context.of("trace-id", traceId));
  }

  private Mono<Void> executeFastQuery(Connection connection, int iteration) {
    // Alternates between inserts and selects to mix the workload type
    String sql = (iteration % 2 == 0)
        ? "INSERT INTO dbo.HighThroughput (val) VALUES (@val)"
        : "SELECT 1 AS tick";

    return Flux.from(connection.createStatement(sql).bind("@val", iteration).execute())
        .flatMap(Result::getRowsUpdated)
        .onErrorResume(e -> {
          System.err.println("Request " + iteration + " failed: " + e.getMessage());
          return Mono.empty();
        })
        .then();
  }
}