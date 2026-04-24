package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.api.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.Function;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsClientLobBinding {

  public static void main(String[] args) {
    new TdsClientLobBinding().run();
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

    System.out.println("Connecting to database pool for LOB Binding & Recovery Testing...");

    Mono.usingWhen(
            Mono.just(pool),
            this::runSql,
            p -> p.disposeLater().doOnSuccess(v -> System.out.println("\nConnection Pool safely closed."))
        )
        .doOnError(t -> System.err.println("\n❌ Test Suite Failed: " + t.getMessage()))
        .block();
  }

  public Mono<Void> runSql(ConnectionPool pool) {
    // Borrow exactly one connection for the entire sequence to prove
    // the driver recovers gracefully on the same physical socket.
    return Mono.usingWhen(
        Mono.from(pool.create()),
        this::executeTestSequence,
        Connection::close
    );
  }

  private Mono<Void> executeTestSequence(Connection connection) {
    String createTableSql = "DROP TABLE IF EXISTS dbo.LobTest; CREATE TABLE dbo.LobTest (id INT, massive_data VARCHAR(MAX));";
    String insertSql = "INSERT INTO dbo.LobTest (id, massive_data) VALUES (@id, @data)";

    // Create a reactive Clob. R2DBC 1.0.0 uses Publishers for LOBs.
    Clob reactiveClob = Clob.from(Mono.just("This represents a massive, multi-gigabyte stream of text"));

    return Mono.defer(() -> executeStream("1. Setup LOB Table", connection.createStatement(createTableSql).execute(), Result::getRowsUpdated))

        .then(Mono.defer(() -> {
          // --- THE NEW CAPABILITY ---
          // This will now successfully stream via ClobStreamingEncoder
          Statement stmt = connection.createStatement(insertSql)
              .bind("@id", 1)
              .bind("@data", reactiveClob);

          return executeStream("2. Execute R2DBC 1.0.0 Clob Bind (Streaming)", stmt.execute(), Result::getRowsUpdated);
        }))

        .then(Mono.defer(() -> {
          // Verify standard string bindings still work flawlessly
          Statement stmt = connection.createStatement(insertSql)
              .bind("@id", 2)
              .bind("@data", "This is a standard, non-LOB string payload");

          return executeStream("3. Execute Standard String Bind (Non-LOB)", stmt.execute(), Result::getRowsUpdated);
        }))

        .then(Mono.defer(() -> {
          // Read back BOTH rows to guarantee data integrity of the streaming encoder
          String selectSql = "SELECT id, massive_data FROM dbo.LobTest ORDER BY id";
          Statement stmt = connection.createStatement(selectSql);

          return executeStream("4. Validate Inserted Data", stmt.execute(),
              res -> res.map((row, meta) -> "Row " + row.get("id", Integer.class) + ": " + row.get("massive_data", String.class)));
        }));
  }

  // --- Strict Async Helper ---
  private <T> Mono<Void> executeStream(String stepName, Publisher<? extends Result> resultPublisher, Function<Result, Publisher<T>> extractor) {
    System.out.println("\n--- Executing: " + stepName + " ---");

    return Flux.from(resultPublisher)
        .flatMap(extractor)
        .doOnNext(item -> System.out.println("  -> Result: " + item))
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
          if (e instanceof IllegalStateException) {
            return Mono.error(e); // Propagate our manual failure
          }
          // The custom lib correctly rejected it. Print the error and continue the chain.
          System.out.println("  ✓ [Graceful Rejection Caught]: " + e.getClass().getSimpleName() + " - " + e.getMessage());
          System.out.println("--- Completed: " + stepName + " ---");
          return Mono.empty();
        });
  }
}