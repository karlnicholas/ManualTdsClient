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
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.api.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.function.Function;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsClientLobTest {
  public static void main(String[] args) {
    new TdsClientLobTest().run();
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

    // Configure a simple pool for the standalone client execution
    ConnectionPoolConfiguration poolConfiguration = ConnectionPoolConfiguration.builder(connectionFactory)
        .initialSize(2)
        .maxSize(10)
        .maxIdleTime(Duration.ofMinutes(10))
        .build();

    ConnectionPool pool = new ConnectionPool(poolConfiguration);

    System.out.println("Connecting to pool for Comprehensive Binding Matrix & Way Testing...");

    // Manage the Pool lifecycle and fail-fast on errors
    Mono.usingWhen(
            Mono.just(pool),
            this::runSql,
            p -> p.disposeLater().doOnSuccess(v -> System.out.println("\nTests complete. Connection pool closed."))
        )
        .doOnError(t -> System.err.println("\n❌ Test Suite Failed: " + t.getMessage()))
        .block();
  }

  /**
   * Overloaded method: Takes a ConnectionPool, borrows a single connection,
   * runs the tests, and safely releases the connection back to the pool.
   */
  public Mono<Void> runSql(ConnectionPool pool) {
    return Mono.usingWhen(
        Mono.from(pool.create()),
        this::runSql,
        Connection::close
    );
  }

  public Mono<Void> runSql(Connection connection) {
    // Queries
    String q1 = "SET TEXTSIZE -1; SELECT test_varchar_max FROM dbo.AllDataTypes where id=1;";
    String q2 = "SET TEXTSIZE -1; SELECT test_nvarchar_max FROM dbo.AllDataTypes where id=1;";
    String q3 = "SET TEXTSIZE -1; SELECT test_varbinary_max FROM dbo.AllDataTypes where id=1;";
    String q4 = "SET TEXTSIZE -1; SELECT REPLICATE(CAST('A' AS VARCHAR(MAX)), 2000000000) AS LargeString;";
    String q5 = "SET TEXTSIZE -1; SELECT CAST(REPLICATE(CAST('A' AS VARCHAR(MAX)), 2000000000) AS VARBINARY(MAX)) AS LargeBinary;";
    String q6 = "SET TEXTSIZE -1; SELECT REPLICATE(CAST(N'あ' AS NVARCHAR(MAX)), 1000000000) AS LargeNString;";

    // Extractors
    Function<Result, Publisher<Integer>> syncString = res -> res.map((row, meta) -> {
      String val = row.get(0, String.class);
      return val == null ? 0 : val.length();
    });

    Function<Result, Publisher<Long>> asyncClob = res -> Flux.from(res.map((row, meta) -> {
      Clob clob = row.get(0, Clob.class);
      if (clob == null) return Mono.just(0L);
      return Flux.from(clob.stream()).reduce(0L, (acc, chunk) -> acc + chunk.length());
    })).flatMap(m -> m);

    Function<Result, Publisher<Integer>> syncBlob = res -> res.map((row, meta) -> {
      ByteBuffer val = row.get(0, ByteBuffer.class);
      return val == null ? 0 : val.remaining();
    });

    Function<Result, Publisher<Long>> asyncBlob = res -> Flux.from(res.map((row, meta) -> {
      Blob blob = row.get(0, Blob.class);
      if (blob == null) return Mono.just(0L);
      return Flux.from(blob.stream()).reduce(0L, (acc, chunk) -> acc + chunk.remaining());
    })).flatMap(m -> m);

    // Sequential Test Execution Chain
    return Mono.defer(() -> executeStream("1A. VARCHAR(MAX) - Sync String", connection.createStatement(q1).execute(), syncString))
        .then(Mono.defer(() -> executeStream("1B. VARCHAR(MAX) - Async Clob", connection.createStatement(q1).execute(), asyncClob)))

        .then(Mono.defer(() -> executeStream("2A. NVARCHAR(MAX) - Sync String", connection.createStatement(q2).execute(), syncString)))
        .then(Mono.defer(() -> executeStream("2B. NVARCHAR(MAX) - Async Clob", connection.createStatement(q2).execute(), asyncClob)))

        .then(Mono.defer(() -> executeStream("3A. VARBINARY(MAX) - Sync ByteBuffer", connection.createStatement(q3).execute(), syncBlob)))
        .then(Mono.defer(() -> executeStream("3B. VARBINARY(MAX) - Async Blob", connection.createStatement(q3).execute(), asyncBlob)))

        .then(Mono.defer(() -> executeStream("4A. 1 GB Generated String - Sync String", connection.createStatement(q4).execute(), syncString)))
        .then(Mono.defer(() -> executeStream("4B. 1 GB Generated String - Async Clob", connection.createStatement(q4).execute(), asyncClob)))

        .then(Mono.defer(() -> executeStream("5A. 100 MB Generated Binary - Sync ByteBuffer", connection.createStatement(q5).execute(), syncBlob)))
        .then(Mono.defer(() -> executeStream("5B. 100 MB Generated Binary - Async Blob", connection.createStatement(q5).execute(), asyncBlob)))

    // Add to your execution chain
        .then(Mono.defer(() -> executeStream("6A. 2 GB Generated NVARCHAR - Sync String", connection.createStatement(q6).execute(), syncString)))
        .then(Mono.defer(() -> executeStream("6B. 2 GB Generated NVARCHAR - Async Clob", connection.createStatement(q6).execute(), asyncClob)));
  }

  // --- The Universal Async Helper ---
  private <T> Mono<Void> executeStream(String stepName, Publisher<? extends Result> resultPublisher, Function<Result, Publisher<T>> extractor) {
    System.out.println("\n--- Executing: " + stepName + " ---");

    return Flux.from(resultPublisher)
        .flatMap(extractor)
        .doOnNext(length -> System.out.println("  -> Final Length: " + length))
        .doOnError(error -> System.err.println("  -> [ERROR] " + error.getMessage()))
        .doOnComplete(() -> System.out.println("--- Completed: " + stepName + " ---"))
        .then()
        .onErrorResume(e -> Mono.empty()); // CRITICAL: Allows test suite to continue if Sync OOMs
  }
}