package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.ValidationDepth;
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.api.TdsLibOptions;
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

public class TdsClientBatchMisbehaved {

  public static void main(String[] args) {
    new TdsClientBatchMisbehaved().run();
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

    System.out.println("Connecting to database pool for Misbehaved Client Testing...");

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

  public Mono<Void> runSql(Connection connection, UUID traceId) {

    return Mono.defer(() -> {
          System.out.println("\n--- Executing: 1. Setup Data for Abandonment Test ---");
          String setup = """
              DROP TABLE IF EXISTS dbo.Misbehaved;
              CREATE TABLE dbo.Misbehaved (id INT IDENTITY(1,1), dummy VARCHAR(50));
              INSERT INTO dbo.Misbehaved (dummy) VALUES ('A'), ('B'), ('C'), ('D'), ('E'), ('F'), ('G'), ('H'), ('I'), ('J');
              """;
          return executeStream("1. Setup", connection.createStatement(setup).execute(), Result::getRowsUpdated);
        })

        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 2. Stream Abandonment (Take 1) ---");
          // The database sends 10 rows + a DONE token.
          // We intentionally subscribe to only 1 row and cancel the rest.
          // The driver MUST silently consume and discard the remaining 9 rows + token off the TCP wire
          // to leave the connection in a clean state.
          return Flux.from(connection.createStatement("SELECT * FROM dbo.Misbehaved").execute())
              .flatMap(result -> result.map((row, meta) -> row.get("dummy", String.class)))
              .take(1) // Cancels the upstream subscription immediately after the 1st item
              .doOnNext(item -> System.out.println("  -> Read: " + item + " (and abandoned the rest)"))
              .then();
        }))

        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 3. Post-Abandonment Sanity Check ---");
          // If the previous test failed to drain the wire, THIS query will fail with protocol errors
          // because it will read the abandoned rows instead of its own results.
          return executeStream("3. Sanity Check", connection.createStatement("SELECT 'Wire is Clean'").execute(),
              res -> res.map((row, meta) -> row.get(0, String.class)));
        }))

        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 4. Unconsumed Result Set ---");
          // We execute a query but we NEVER map the rows. We just ask for rows updated.
          // The driver must realize the user ignored the RowSegment stream and discard it automatically.
          return executeStream("4. Unconsumed Rows", connection.createStatement("SELECT * FROM dbo.Misbehaved").execute(), Result::getRowsUpdated);
        }))

        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 5. Connection Validation ---");
          return Mono.from(connection.validate(ValidationDepth.REMOTE))
              .doOnNext(isValid -> {
                if (isValid) {
                  System.out.println("  -> SUCCESS: Connection survived abuse and is healthy.");
                } else {
                  System.err.println("  -> FAILED: Connection was corrupted by misbehavior.");
                }
              })
              .then();
        }))
        .contextWrite(Context.of("trace-id", traceId));
  }

  private <T> Mono<Void> executeStream(String stepName, Publisher<? extends Result> resultPublisher, Function<Result, Publisher<T>> extractor) {
    return Flux.from(resultPublisher)
        .flatMap(extractor)
        .doOnNext(item -> System.out.println("  -> " + item))
        .doOnError(error -> System.err.println("[" + stepName + "] ❌ FAILED: " + error.getMessage()))
        .then();
  }
}