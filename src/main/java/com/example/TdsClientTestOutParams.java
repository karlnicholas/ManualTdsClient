package com.example;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.OutParameters;
import io.r2dbc.spi.OutParametersMetadata;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.api.TdsConnectionFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsClientTestOutParams {
  public static void main(String[] args) throws Exception {
    new TdsClientTestOutParams().run();
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
    String outSql = "SELECT @count = COUNT(*), @sum = SUM(postCount), @average = AVG(postCount) FROM dbo.users";

    // 2. Return the Mono<Void> representing the entire operation
    return executeStreamNoLatch("5. Read Clob Length", connection.createStatement(outSql)
            .bind("@count", Parameters.out(R2dbcType.BIGINT))
            .bind("@sum", Parameters.out(R2dbcType.BIGINT))
            .bind("@average", Parameters.out(R2dbcType.BIGINT)).execute(),
        res -> res.flatMap(segment -> {
              if (segment instanceof Result.OutSegment outSeg) {
                OutParameters out = outSeg.outParameters();
                return Flux.just(rvOutMapper.apply(out, out.getMetadata()));
              }
              return Flux.empty();
            }))
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

  BiFunction<OutParameters, OutParametersMetadata, List<Integer>> rvOutMapper = (out, meta) -> List.of(
      out.get(0, Integer.class),
      out.get(1, Integer.class),
      out.get(2, Integer.class)
  );

}