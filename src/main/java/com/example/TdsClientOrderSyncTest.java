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

import java.util.concurrent.CountDownLatch;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsClientOrderSyncTest {
  public static void main(String[] args) throws Exception {
    new TdsClientOrderSyncTest().run();
  }

  private void run() throws Exception {
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
        .doOnNext(conn -> {
          try {
            runSql(conn);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        })
        .doOnError(t -> System.err.println("Connection/Run Failed: " + t.getMessage()))
        .block();
  }

  private void runSql(Connection connection) throws InterruptedException {
    // 1. Discover columns
      // A. Random number of columns
      String query = "SELECT id, test_char FROM dbo.AllDataTypes where id=1";


      executeQuery("Test", connection.createStatement(query).execute());

    Mono.from(connection.close())
        .doFinally(signal -> System.out.println("Connection safely closed."))
        .subscribe();
  }


  private void executeQuery(String stepName, Publisher<? extends Result> resultPublisher)
      throws InterruptedException {
//    System.out.println("\n--- " + stepName + " ---");
    CountDownLatch latch = new CountDownLatch(1);

    Flux.from(resultPublisher)
        .flatMap(result -> result.map((row, meta) -> {
          StringBuilder sb = new StringBuilder();
          Object value1 = row.get(1, String.class);
          Object value0 = row.get(0, String.class);
          return sb.toString().trim();
        }))
        .subscribe(
//            item -> System.out.println("  → " + item),
            item -> {},
            error -> {
              System.err.println("[" + stepName + "] Error: " + error.getMessage());
              latch.countDown();
            },
            () -> {
//              System.out.println("--- Done: " + stepName + " ---");
              latch.countDown();
            }
        );

    latch.await();
  }
}