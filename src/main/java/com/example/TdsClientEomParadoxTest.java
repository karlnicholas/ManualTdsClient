package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Blob;
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

public class TdsClientEomParadoxTest {

  public static void main(String[] args) {
    new TdsClientEomParadoxTest().run();
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

    System.out.println("Connecting to database pool for EOM Paradox Testing...");

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
    String createTableSql = "DROP TABLE IF EXISTS dbo.EomTest; " +
        "CREATE TABLE dbo.EomTest (" +
        "  id INT, " +
        "  test_name VARCHAR(100), " +
        "  massive_binary VARBINARY(MAX) NULL" +
        ");";

    String insertSql = "INSERT INTO dbo.EomTest (id, test_name, massive_binary) VALUES (@id, @name, @data)";

    return Mono.defer(() -> executeStream("1. Setup EOM Paradox Table", connection.createStatement(createTableSql).execute(), Result::getRowsUpdated))

// --- TEST 1: Forced Fragmentation (Single 1MB Buffer) ---
        .then(Mono.defer(() -> {
          Blob massiveBlob = createMockBlob(1024 * 1024);
          Statement stmt = connection.createStatement(insertSql)
              .bind("@id", 1)
              .bind("@name", "Forced Fragmentation (1MB)")
              .bind("@data", massiveBlob);
          return executeStream("2. Execute 1MB Blob Bind (Single Buffer)", stmt.execute(), Result::getRowsUpdated);
        }))

        // --- TEST 2: Simulated File Streaming (1MB in 4KB Chunks) ---
        // Dynamically emits multiple onNext signals, proving the framer
        // can accumulate small chunks across reactive boundaries.
        .then(Mono.defer(() -> {
          Blob chunkedBlob = createChunkedMockBlob(1024 * 1024, 4096);
          Statement stmt = connection.createStatement(insertSql)
              .bind("@id", 2)
              .bind("@name", "Chunked Streaming (1MB in 4KB)")
              .bind("@data", chunkedBlob);
          return executeStream("3. Execute 1MB Chunked Blob Bind", stmt.execute(), Result::getRowsUpdated);
        }))

        // --- TEST 3: The Empty Stream (0 Bytes) ---
        .then(Mono.defer(() -> {
          Blob emptyBlob = Blob.from(Mono.empty());
          Statement stmt = connection.createStatement(insertSql)
              .bind("@id", 3)
              .bind("@name", "Empty Stream (0 Bytes)")
              .bind("@data", emptyBlob);
          return executeStream("4. Execute Empty Blob Bind", stmt.execute(), Result::getRowsUpdated);
        }))

        // --- TEST 4: The Zero-Byte EOM Paradox ---
        .then(Mono.defer(() -> {
          Blob exactBoundaryBlob = createMockBlob(7532);
          Statement stmt = connection.createStatement(insertSql)
              .bind("@id", 4)
              // Name string is strictly 27 chars to guarantee overhead remains exactly 444 bytes
              .bind("@name", "Exact Zero-Byte EOM Paradox")
              .bind("@data", exactBoundaryBlob);
          return executeStream("5. Execute 7532 Byte Blob Bind (Zero-Byte EOM)", stmt.execute(), Result::getRowsUpdated);
        }))

        // --- VALIDATION ---
        .then(Mono.defer(() -> {
          String selectSql = "SET TEXTSIZE -1; SELECT id, test_name, massive_binary FROM dbo.EomTest ORDER BY id";
          Statement stmt = connection.createStatement(selectSql);

          return executeStream("6. Validate Inserted Data", stmt.execute(),
              res -> res.map((row, meta) -> {
                int id = row.get("id", Integer.class);
                String name = row.get("test_name", String.class);
                byte[] bin = row.get("massive_binary", byte[].class);

                String binInfo = (bin != null) ? "Size: " + bin.length + " bytes" : "null/empty";

                return String.format("Row %d: [%s] -> Binary=%s", id, name, binInfo);
              }));
        }));
  }

  /**
   * Legacy test method: Allocates the entire array in memory at once.
   */
  private Blob createMockBlob(int sizeInBytes) {
    byte[] mockBytes = new byte[sizeInBytes];
    Arrays.fill(mockBytes, (byte) 0xAA);
    return Blob.from(Mono.just(ByteBuffer.wrap(mockBytes)));
  }

  /**
   * Real-world simulation: Dynamically generates `onNext` signals of exactly `chunkSize`,
   * ensuring that only `chunkSize` bytes exist in memory at any given time.
   */
  private Blob createChunkedMockBlob(int totalSizeInBytes, int chunkSizeInBytes) {
    return Blob.from(Flux.generate(
        () -> 0, // Initial state: 0 bytes emitted
        (emittedBytes, sink) -> {
          if (emittedBytes >= totalSizeInBytes) {
            sink.complete();
            return emittedBytes;
          }

          // Calculate how much to emit this round (either chunkSize or whatever is left)
          int bytesToEmit = Math.min(chunkSizeInBytes, totalSizeInBytes - emittedBytes);
          byte[] chunk = new byte[bytesToEmit];
          Arrays.fill(chunk, (byte) 0xBB); // Distinct payload byte

          sink.next(ByteBuffer.wrap(chunk));

          return emittedBytes + bytesToEmit; // Return updated state
        }
    ));
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
}