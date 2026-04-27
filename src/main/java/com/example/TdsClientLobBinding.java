package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Blob;
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

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
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
    return Mono.usingWhen(
        Mono.from(pool.create()),
        this::executeTestSequence,
        Connection::close
    );
  }

  private Mono<Void> executeTestSequence(Connection connection) {
    // 1. Expanded schema to test both NVARCHAR(MAX) and VARBINARY(MAX)
// 1. Expanded schema to test both NVARCHAR(MAX) and VARBINARY(MAX)
    String createTableSql = "DROP TABLE IF EXISTS dbo.LobTest; " +
        "CREATE TABLE dbo.LobTest (" +
        "  id INT, " +
        "  massive_text VARCHAR(MAX) NULL, " +
        "  massive_binary VARBINARY(MAX) NULL" +
        ");";

    String insertTextSql = "INSERT INTO dbo.LobTest (id, massive_text) VALUES (@id, @data)";
    String insertBinarySql = "INSERT INTO dbo.LobTest (id, massive_binary) VALUES (@id, @data)";

    // 2. Mock Data Generation
    Clob reactiveClob = Clob.from(Mono.just("This represents a massive, multi-gigabyte stream of text"));

    byte[] mockBytes = new byte[1024 * 5]; // 5KB of mock binary data
    Arrays.fill(mockBytes, (byte) 0xAA);
    Blob reactiveBlob = Blob.from(Mono.just(ByteBuffer.wrap(mockBytes)));

    return Mono.defer(() -> executeStream("1. Setup LOB Table", connection.createStatement(createTableSql).execute(), Result::getRowsUpdated))

        // --- CLOB TESTS ---
        .then(Mono.defer(() -> {
          Statement stmt = connection.createStatement(insertTextSql)
              .bind("@id", 1)
              .bind("@data", reactiveClob);
          return executeStream("2. Execute R2DBC 1.0.0 Clob Bind (Streaming)", stmt.execute(), Result::getRowsUpdated);
        }))
        .then(Mono.defer(() -> {
          Statement stmt = connection.createStatement(insertTextSql)
              .bind("@id", 2)
              .bind("@data", "This is a standard, non-LOB string payload");
          return executeStream("3. Execute Standard String Bind (Non-LOB)", stmt.execute(), Result::getRowsUpdated);
        }))

        // --- BLOB TESTS ---
        .then(Mono.defer(() -> {
          Statement stmt = connection.createStatement(insertBinarySql)
              .bind("@id", 3)
              .bind("@data", reactiveBlob);
          return executeStream("4. Execute R2DBC 1.0.0 Blob Bind (Streaming)", stmt.execute(), Result::getRowsUpdated);
        }))
        .then(Mono.defer(() -> {
          byte[] standardBytes = new byte[]{0x01, 0x02, 0x03, 0x04};
          Statement stmt = connection.createStatement(insertBinarySql)
              .bind("@id", 4)
              .bind("@data", ByteBuffer.wrap(standardBytes)); // Or byte[] if your scalar encoder supports it directly
          return executeStream("5. Execute Standard Binary Bind (Non-LOB)", stmt.execute(), Result::getRowsUpdated);
        }))

        // --- VALIDATION ---
        .then(Mono.defer(() -> {
          String selectSql = "SET TEXTSIZE -1; SELECT id, massive_text, massive_binary FROM dbo.LobTest ORDER BY id";
          Statement stmt = connection.createStatement(selectSql);

          return executeStream("6. Validate Inserted Data", stmt.execute(),
              res -> res.map((row, meta) -> {
                int id = row.get("id", Integer.class);
                String text = row.get("massive_text", String.class);

                // Assuming your row decoder maps varbinary to byte[] or ByteBuffer
                byte[] bin = row.get("massive_binary", byte[].class);
                String binInfo = (bin != null) ? "Binary Size: " + bin.length + " bytes" : "null";

                return String.format("Row %d: Text=[%s] | Binary=[%s]", id, text, binInfo);
              }));
        }));
  }

  private <T> Mono<Void> executeStream(String stepName, Publisher<? extends Result> resultPublisher, Function<Result, Publisher<T>> extractor) {
    System.out.println("\n--- Executing: " + stepName + " ---");

    return Flux.from(resultPublisher)
        .flatMap(extractor)
        .doOnNext(item -> System.out.println("  -> Result: " + item))
        .doOnError(error -> System.err.println("[" + stepName + "] ❌ FAILED: " + error.getMessage()))
        .doOnComplete(() -> System.out.println("--- Completed: " + stepName + " ---"))
        .then();
  }

  private <T> Mono<Void> executeExpectedErrorStream(String stepName, Publisher<? extends Result> resultPublisher, Function<Result, Publisher<T>> extractor) {
    System.out.println("\n--- Executing [EXPECTS ERROR]: " + stepName + " ---");

    return Flux.from(resultPublisher)
        .flatMap(extractor)
        .doOnNext(item -> System.out.println("  -> " + item))
        .then(Mono.<Void>error(new IllegalStateException("Step '" + stepName + "' was expected to throw an error, but it succeeded!")))
        .onErrorResume(e -> {
          if (e instanceof IllegalStateException) {
            return Mono.error(e);
          }
          System.out.println("  ✓ [Graceful Rejection Caught]: " + e.getClass().getSimpleName() + " - " + e.getMessage());
          System.out.println("--- Completed: " + stepName + " ---");
          return Mono.empty();
        });
  }
}