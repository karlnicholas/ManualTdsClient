package com.example;

import io.r2dbc.spi.*;
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.api.TdsConnectionFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class TdsClientSelectOne {
  public static void main(String[] args) throws Exception {
    new TdsClientSelectOne().run();
  }

  private void run() throws Exception {
    ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(ConnectionFactoryOptions.DRIVER, "javatdslib")
        .option(HOST, "localhost")
        .option(PORT, 1433)
        .option(PASSWORD, "reactnonreact")
        .option(USER, "reactnonreact")
        .option(DATABASE, "reactnonreact")
        .option(TdsConnectionFactory.TRUST_SERVER_CERTIFICATE, true)
        .build());

    System.out.println("Connecting to database...");

    // Use flatMap to chain the asynchronous query to the connection lifecycle
    Mono.from(connectionFactory.create())
        .flatMap(conn -> {
          try {
            return runSql(conn); // Returns the Mono<Void> to be chained
          } catch (InterruptedException e) {
            return Mono.error(e);
          }
        })
        .doOnError(t -> System.err.println("Connection/Run Failed: " + t.getMessage()))
        .block(); // Now this blocks until runSql's Mono<Void> completes!
  }

  @SuppressWarnings("JpaQueryApiInspection")
  private Mono<Void> runSql(Connection connection) throws InterruptedException {
    // 2. Return the Mono<Void> representing the entire operation
    return executeStreamNoLatch("5. Read Clob Length", connection.createStatement(querySql).execute(),
        res -> Flux.from(res.map((row, meta) -> {
          Clob clob = row.get(0, Clob.class);
          if (clob == null) {
            return Mono.just(0L);
          }

          // Stream the chunks and count the total characters
          return Flux.from(clob.stream())
              .reduce(0L, (accumulator, chunk) -> accumulator + chunk.length());

        })).flatMap(mono -> mono) // Flatten the Mono<Long> into the main Flux
    )
        .then(Mono.from(connection.close())) // 3. Ensure connection closes AFTER the stream completes
        .doFinally(signal -> System.out.println("Connection safely closed."));
  }

  // --- The Universal Async Helper (LATCH-FREE) ---

  private <T> Mono<Void> executeStreamNoLatch(String stepName, Publisher<? extends Result> resultPublisher, Function<Result, Publisher<T>> extractor) {
    System.out.println("\n--- Executing: " + stepName + " ---");

    // 4. Build the reactive pipeline without subscribing or blocking
    return Flux.from(resultPublisher)
        .flatMap(extractor)
        .doOnNext(item -> System.out.println("  -> " + item))
        .doOnError(error -> System.err.println("[" + stepName + "] Stream Error: " + error.getMessage()))
        .doOnComplete(() -> System.out.println("--- Completed: " + stepName + " ---"))
        .then(); // .then() converts Flux<T> into a Mono<Void> that completes when the Flux finishes
  }

  // --- The Universal Async Helper using Reactor Flux ---

  private <T> void executeStream(String stepName, Publisher<? extends Result> resultPublisher, Function<Result, Publisher<T>> extractor) throws InterruptedException {
    System.out.println("\n--- Executing: " + stepName + " ---");
    CountDownLatch latch = new CountDownLatch(1);

    Flux.from(resultPublisher)
        .flatMap(extractor)
        .subscribe(
            item -> System.out.println("  -> " + item),
            error -> {
              System.err.println("[" + stepName + "] Stream Error: " + error.getMessage());
              latch.countDown();
            },
            () -> {
              System.out.println("--- Completed: " + stepName + " ---");
              latch.countDown();
            }
        );

    latch.await();
  }

  // --- Mappers ---

  BiFunction<Row, RowMetadata, AllDataTypesRecord> allDataTypesMapper = (row, meta) -> new AllDataTypesRecord(
      row.get(0, Integer.class),          row.get(1, Boolean.class),
      row.get(2, Byte.class),             row.get(3, Short.class),
      row.get(4, Integer.class),          row.get(5, Long.class),
      row.get(6, BigDecimal.class),       row.get(7, BigDecimal.class),
      row.get(8, BigDecimal.class),       row.get(9, BigDecimal.class),
      row.get(10, Float.class),           row.get(11, Double.class),
      row.get(12, LocalDate.class),       row.get(13, LocalTime.class),
      row.get(14, LocalDateTime.class),   row.get(15, LocalDateTime.class),
      row.get(16, LocalDateTime.class),   row.get(17, OffsetDateTime.class),
      row.get(18, String.class),
      row.get(19, String.class),
      row.get(20, String.class),          row.get(21, String.class),
      row.get(22, String.class),          row.get(23, String.class),
      row.get(24, String.class),          row.get(25, byte[].class),
      row.get(26, byte[].class),          row.get(27, byte[].class),
      row.get(28, byte[].class),          row.get(29, UUID.class),
      row.get(30, String.class)
  );


//  private static final String querySql = """
//    SET TEXTSIZE -1;
//    SELECT REPLICATE(CAST('A' AS VARCHAR(MAX)), 1000000000) AS LargeString;
//    """;

//  private static final String querySql = """
//  SET TEXTSIZE -1;
//  -- Replicate a char, then cast the massive result to VARBINARY(MAX)
//  SELECT CAST(REPLICATE(CAST('A' AS VARCHAR(MAX)), 104857600) AS VARBINARY(MAX)) AS LargeBinary;
//  """;
//    SELECT test_varchar_max FROM dbo.AllDataTypes where id=1;
//SELECT REPLICATE(CAST('A' AS VARCHAR(MAX)), 1000000000) AS LargeString;

  private static final String querySql = """
    SET TEXTSIZE -1;
    SELECT test_varchar_max FROM dbo.AllDataTypes where id=1;
    """;
}