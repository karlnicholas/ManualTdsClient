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
import io.r2dbc.spi.Row;
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.api.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.function.Function;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class TdsClientLobExtraction {

  /**
   * Defines the scale of the generated LOBs to test both the "working path"
   * and the extreme "JVM/Socket boundary" path.
   */
  public enum Scale {
    NORMAL(2_500_000, 2_500_000, 2_500_000),
    MASSIVE(2_000_000_000, 2_000_000_000, 1_000_000_000);

    final long stringCount;
    final long binaryCount;
    final long nstringCount;

    Scale(long stringCount, long binaryCount, long nstringCount) {
      this.stringCount = stringCount;
      this.binaryCount = binaryCount;
      this.nstringCount = nstringCount;
    }
  }

  public static void main(String[] args) {
    new TdsClientLobExtraction().run();
  }

  private void run() {
    ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(DRIVER, "javatdslib")
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

    System.out.println("Connecting to pool for Consolidated LOB Extraction Suite...");

    Mono.usingWhen(
            Mono.just(pool),
            this::runSql,
            p -> p.disposeLater().doOnSuccess(v -> System.out.println("\n✅ All extraction tests complete. Connection pool closed."))
        )
        .doOnError(t -> System.err.println("\n❌ Test Suite Failed: " + t.getMessage()))
        .block();
  }

  public Mono<Void> runSql(ConnectionPool pool) {
    return Mono.usingWhen(
        Mono.from(pool.create()),
        connection ->
            runExtractionTests(connection, Scale.NORMAL)
                .then(runExtractionTests(connection, Scale.MASSIVE))
                .then(testMultiLobColumnDecoding(connection)),
        Connection::close
    );
  }

  // ---------------------------------------------------------------------------------------
  // SCALED EXTRACTION TESTS
  // ---------------------------------------------------------------------------------------
  private Mono<Void> runExtractionTests(Connection connection, Scale scale) {
    System.out.println("\n=======================================================");
    System.out.println("  RUNNING EXTRACTION TESTS - SCALE: " + scale.name());
    System.out.println("=======================================================");

    // Static DB queries
    String q1 = "SET TEXTSIZE -1; SELECT test_varchar_max FROM dbo.AllDataTypes where id=1;";
    String q2 = "SET TEXTSIZE -1; SELECT test_nvarchar_max FROM dbo.AllDataTypes where id=1;";
    String q3 = "SET TEXTSIZE -1; SELECT test_varbinary_max FROM dbo.AllDataTypes where id=1;";

    // Dynamic Scaled Queries
    String q4 = "SET TEXTSIZE -1; SELECT REPLICATE(CAST('A' AS VARCHAR(MAX)), " + scale.stringCount + ") AS LargeString;";
    String q5 = "SET TEXTSIZE -1; SELECT CAST(REPLICATE(CAST('A' AS VARCHAR(MAX)), " + scale.binaryCount + ") AS VARBINARY(MAX)) AS LargeBinary;";
    String q6 = "SET TEXTSIZE -1; SELECT REPLICATE(CAST(N'あ' AS NVARCHAR(MAX)), " + scale.nstringCount + ") AS LargeNString;";

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

    // Sequential Execution Chain
    return Mono.defer(() -> executeStream("1A. VARCHAR(MAX) - Sync String", connection.createStatement(q1).execute(), syncString))
        .then(Mono.defer(() -> executeStream("1B. VARCHAR(MAX) - Async Clob", connection.createStatement(q1).execute(), asyncClob)))

        .then(Mono.defer(() -> executeStream("2A. NVARCHAR(MAX) - Sync String", connection.createStatement(q2).execute(), syncString)))
        .then(Mono.defer(() -> executeStream("2B. NVARCHAR(MAX) - Async Clob", connection.createStatement(q2).execute(), asyncClob)))

        .then(Mono.defer(() -> executeStream("3A. VARBINARY(MAX) - Sync ByteBuffer", connection.createStatement(q3).execute(), syncBlob)))
        .then(Mono.defer(() -> executeStream("3B. VARBINARY(MAX) - Async Blob", connection.createStatement(q3).execute(), asyncBlob)))

        .then(Mono.defer(() -> executeStream("4A. Generated String - Sync String", connection.createStatement(q4).execute(), syncString)))
        .then(Mono.defer(() -> executeStream("4B. Generated String - Async Clob", connection.createStatement(q4).execute(), asyncClob)))

        .then(Mono.defer(() -> executeStream("5A. Generated Binary - Sync ByteBuffer", connection.createStatement(q5).execute(), syncBlob)))
        .then(Mono.defer(() -> executeStream("5B. Generated Binary - Async Blob", connection.createStatement(q5).execute(), asyncBlob)))

        .then(Mono.defer(() -> executeStream("6A. Generated NVARCHAR - Sync String", connection.createStatement(q6).execute(), syncString)))
        .then(Mono.defer(() -> executeStream("6B. Generated NVARCHAR - Async Clob", connection.createStatement(q6).execute(), asyncClob)));
  }

  // ---------------------------------------------------------------------------------------
  // MULTI-COLUMN LOB DECODING BUG TEST
  // ---------------------------------------------------------------------------------------
  private Mono<Void> testMultiLobColumnDecoding(Connection connection) {
    System.out.println("\n=======================================================");
    System.out.println("  RUNNING MULTI-COLUMN LOB DECODING TEST");
    System.out.println("=======================================================");

    String sql = "SET TEXTSIZE -1; SELECT " +
        "test_varchar_max, test_nvarchar_max, test_varbinary_max " +
        "FROM dbo.AllDataTypes WHERE id = 1;";

    return Flux.from(connection.createStatement(sql).execute())
        .flatMap(result -> result.map((row, meta) -> {
          System.out.println("--- Mapping All LOB Columns sequentially from Row ---");

          return streamClob(row, "test_varchar_max").doOnNext(length -> System.out.println("  -> test_varchar_max length: " + length))
              .then(streamClob(row, "test_nvarchar_max").doOnNext(length -> System.out.println("  -> test_nvarchar_max length: " + length)))
              .then(streamBlob(row, "test_varbinary_max").doOnNext(length -> System.out.println("  -> test_varbinary_max length: " + length)));
        }))
        .flatMap(f -> f)
        .then();
  }

  // ---------------------------------------------------------------------------------------
  // HELPERS
  // ---------------------------------------------------------------------------------------
  private <T> Mono<Void> executeStream(String stepName, Publisher<? extends Result> resultPublisher, Function<Result, Publisher<T>> extractor) {
    System.out.println("\n--- Executing: " + stepName + " ---");

    return Flux.from(resultPublisher)
        .flatMap(extractor)
        .doOnNext(length -> System.out.println("  -> Final Length: " + length))
        .doOnError(error -> System.err.println("  -> [ERROR] " + error.getMessage()))
        .doOnComplete(() -> System.out.println("--- Completed: " + stepName + " ---"))
        .then()
        .onErrorResume(e -> Mono.empty()); // CRITICAL: Allows test suite to continue if Sync OOMs on MASSIVE scale
  }

  private Mono<Long> streamClob(Row row, String name) {
    Clob clob = row.get(name, Clob.class);
    if (clob == null) return Mono.just(0L);
    return Flux.from(clob.stream())
        .reduce(0L, (acc, chunk) -> acc + chunk.length());
  }

  private Mono<Long> streamBlob(Row row, String name) {
    Blob blob = row.get(name, Blob.class);
    if (blob == null) return Mono.just(0L);
    return Flux.from(blob.stream())
        .reduce(0L, (acc, chunk) -> acc + chunk.remaining());
  }
}