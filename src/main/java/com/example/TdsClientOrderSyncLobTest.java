package com.example;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.api.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsClientOrderSyncLobTest {
  public static void main(String[] args) {
    new TdsClientOrderSyncLobTest().run();
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
            this::runSql, // Now perfectly matches (Connection) -> Mono<Void>
            conn -> Mono.from(conn.close()).doOnSuccess(v -> System.out.println("\nConnection safely closed."))
        )
        .doOnError(t -> System.err.println("Connection/Run Failed: " + t.getMessage()))
        .block();
  }

  private Mono<Void> runSql(Connection connection) {
    // 1. Discover columns
    // A. Random number of columns
    String query = "SELECT test_varchar_max, test_nvarchar_max FROM dbo.AllDataTypes where id=1";

    return executeQuery("Test", connection.createStatement(query).execute());
  }


  private Mono<Void> executeQuery(String stepName, Publisher<? extends Result> resultPublisher) {
    return Flux.from(resultPublisher)
        .flatMap(result -> result.map((row, meta) -> {
          StringBuilder sb = new StringBuilder();
          // Extract variables as requested
          Object value1 = row.get(0, String.class);
          Object value0 = row.get(1, String.class);
          return sb.toString().trim();
        }))
        .doOnError(error -> System.err.println("[" + stepName + "] Error: " + error.getMessage()))
        .then(); // Converts the Flux to a Mono<Void> signaling completion
  }
}