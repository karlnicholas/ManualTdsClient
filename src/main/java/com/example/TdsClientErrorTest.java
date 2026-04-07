package com.example;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.OutParameters;
import io.r2dbc.spi.OutParametersMetadata;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.api.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsClientErrorTest {
  public static void main(String[] args) throws Exception {
    new TdsClientErrorTest().run();
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

    System.out.println("Connecting to database for Transaction Testing...");

    // usingWhen ensures the connection is safely closed regardless of success or error
    Mono.usingWhen(
            Mono.from(connectionFactory.create()),
            this::runSql,
            conn -> Mono.from(conn.close()).doOnSuccess(v -> System.out.println("\nConnection safely closed."))
        )
        .doOnError(t -> System.err.println("Connection/Run Failed: " + t.getMessage()))
        .block();
  }

  @SuppressWarnings("JpaQueryApiInspection")
  private Mono<Void> runSql(Connection connection) {
    return Mono.defer(() -> executeStream("11. Runtime Error Test", connection.createStatement("SELECT CAST('NotAnInteger' AS INT)").execute(), res -> res.map((row, meta) -> row.get(0, Integer.class))))
        .then(Mono.defer(() -> executeStream("11. Runtime Error Test", connection.createStatement("RAISERROR('This is a fatal runtime exception', 16, 1)").execute(), Result::getRowsUpdated)));
  }

  // --- The Universal Async Helper using Reactor Flux ---

  private <T> Mono<Void> executeStream(String stepName, Publisher<? extends Result> resultPublisher, Function<Result, Publisher<T>> extractor) {
    System.out.println("\n--- Executing: " + stepName + " ---");

    return Flux.from(resultPublisher)
        .flatMap(extractor)
        .doOnNext(item -> System.out.println("  -> " + item))
        .doOnError(error -> System.err.println("[" + stepName + "] Stream Error: " + error.getMessage()))
        .doOnComplete(() -> System.out.println("--- Completed: " + stepName + " ---"))
        .then()
        // CRITICAL: Swallow errors here so the `.then()` chain in runSql continues to the next test.
        // This mimics the latch behavior where doOnError counted down and allowed the main thread to proceed.
        .onErrorResume(e -> Mono.empty());
  }

}