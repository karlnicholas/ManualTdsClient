package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ValidationDepth;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;

public class TdsClientPlpBinary {
  public static void main(String[] args) {
    new TdsClientPlpBinary().run();
  }

  private void run() {
    String r2dbcUrl = "r2dbc:mssql://reactnonreact:reactnonreact@localhost:1433/reactnonreact?trustServerCertificate=true";

    // Constrained to exactly 1 connection.
    // If the Clob stream is not fully drained, or if the parser fails to resume,
    // the second test will instantly hang or deadlock waiting for the lock.
    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(r2dbcUrl))
        .initialSize(1).maxSize(1).build());

    Mono.usingWhen(
            Mono.just(pool),
            this::runSql,
            p -> p.disposeLater().doOnSuccess(v -> System.out.println("\nTests complete. Connection pool closed."))
        )
        .doOnError(t -> System.err.println("\n❌ Test Suite Failed: " + t.getMessage()))
        .block();
  }

  public Mono<Void> runSql(ConnectionPool pool) {
    return Mono.usingWhen(
        Mono.from(pool.create()),
        connection ->
            test_PlpStream(connection, "First Stream", 150_000)
                .then(test_PlpStream(connection, "Second Stream", 250_000)) // Proves the socket recovered!
                .then(checkConnection(connection)),
        Connection::close
    );
  }

  // --- CHAOS TEST 1: PLP Streaming Execution ---
  private Mono<Void> test_PlpStream(Connection connection, String executionLabel, int targetSize) {
    return Mono.defer(() -> {
      System.out.println("\n--- TEST: PLP Stream (" + executionLabel + ") ---");
      System.out.println("  Requesting " + targetSize + " bytes...");

      // The CAST to VARCHAR(MAX) forces the SQL engine to use the PLP length strategy (0xFFFF).
      String sql = "SET TEXTSIZE -1;SELECT CAST(REPLICATE(CAST('A' AS VARCHAR(MAX)), " + targetSize + ") AS VARBINARY(MAX)) AS plp_col;";
//      String sql = "SELECT REPLICATE(CAST('A' AS NVARCHAR(MAX)), " + targetSize + ") AS plp_col";

      return Flux.from(connection.createStatement(sql).execute())
          // 1. Extract the Clob from the row (Triggers the early dispatch handoff)
          .flatMap(res -> res.map((row, metadata) -> row.get(0, Blob.class)))
          // 2. Subscribe to the Clob's network stream
          .flatMap(blob -> {
            AtomicInteger chunkCount = new AtomicInteger(0);

            return Flux.from(blob.stream())
                // Track how many physical emissions the Sink makes to the client
                .doOnNext(chunk -> chunkCount.incrementAndGet())
                // Accumulate the total length to verify data integrity
                .reduce(0, (totalLength, chunk) -> totalLength + chunk.remaining())
                .doOnNext(totalLength -> {
                  System.out.println("  [" + executionLabel + "] Stream drained successfully.");
                  System.out.println("  [" + executionLabel + "] Total Length: " + totalLength);
                  System.out.println("  [" + executionLabel + "] Chunks Emitted: " + chunkCount.get());
                });
          })
          .then();
    });
  }

  // --- CHAOS TEST 2: Network State Validation ---
  private Mono<Void> checkConnection(Connection connection) {
    return Mono.defer(() -> {
      System.out.println("\n--- TEST: Connection Validation ---");
      return Mono.from(connection.validate(ValidationDepth.REMOTE))
          .doOnNext(isValid -> System.out.println("  Connection is valid: " + isValid))
          .then();
    });
  }
}