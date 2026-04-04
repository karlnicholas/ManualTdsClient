package com.example;

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
import java.util.function.Function;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsClientLobTest {
  public static void main(String[] args) throws Exception {
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

    System.out.println("Connecting to database...");

    Mono.from(connectionFactory.create())
        .flatMap(conn -> runSql(conn).onErrorResume(e -> Mono.error(e)))
        .doOnError(t -> System.err.println("Connection/Run Failed: " + t.getMessage()))
        .block();
  }

  private Mono<Void> runSql(Connection connection) {
    // Queries
    String q1 = "SET TEXTSIZE -1; SELECT test_varchar_max FROM dbo.AllDataTypes where id=1;";
    String q2 = "SET TEXTSIZE -1; SELECT test_nvarchar_max FROM dbo.AllDataTypes where id=1;";
    String q3 = "SET TEXTSIZE -1; SELECT test_varbinary_max FROM dbo.AllDataTypes where id=1;";
    String q4 = "SET TEXTSIZE -1; SELECT REPLICATE(CAST('A' AS VARCHAR(MAX)), 1000000000) AS LargeString;";
    String q5 = "SET TEXTSIZE -1; SELECT CAST(REPLICATE(CAST('A' AS VARCHAR(MAX)), 104857600) AS VARBINARY(MAX)) AS LargeBinary;";

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

// Sequential Test Execution Chain using Mono.defer()
    return Mono.defer(() -> executeStream("1A. VARCHAR(MAX) - Sync String", connection.createStatement(q1).execute(), syncString))
//        .then(Mono.defer(() -> executeStream("1B. VARCHAR(MAX) - Async Clob", connection.createStatement(q1).execute(), asyncClob)))
//
//        .then(Mono.defer(() -> executeStream("2A. NVARCHAR(MAX) - Sync String", connection.createStatement(q2).execute(), syncString)))
//        .then(Mono.defer(() -> executeStream("2B. NVARCHAR(MAX) - Async Clob", connection.createStatement(q2).execute(), asyncClob)))
//
//        .then(Mono.defer(() -> executeStream("3A. VARBINARY(MAX) - Sync ByteBuffer", connection.createStatement(q3).execute(), syncBlob)))
//        .then(Mono.defer(() -> executeStream("3B. VARBINARY(MAX) - Async Blob", connection.createStatement(q3).execute(), asyncBlob)))
//
//        .then(Mono.defer(() -> executeStream("4A. 1 GB Generated String - Sync String", connection.createStatement(q4).execute(), syncString)))
        .then(Mono.defer(() -> executeStream("4B. 1 GB Generated String - Async Clob", connection.createStatement(q4).execute(), asyncClob)))

//        .then(Mono.defer(() -> executeStream("5A. 100 MB Generated Binary - Sync ByteBuffer", connection.createStatement(q5).execute(), syncBlob)))
//        .then(Mono.defer(() -> executeStream("5B. 100 MB Generated Binary - Async Blob", connection.createStatement(q5).execute(), asyncBlob)))
//
        // Teardown
        .then(Mono.defer(() -> Mono.from(connection.close())))
        .doFinally(signal -> System.out.println("\nConnection safely closed."));
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