package com.example;

import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
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
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.tdslib.javatdslib.api.TdsConnectionFactory;

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

public class CSharpTdsClientPartial {
  public static void main(String[] args) throws Exception {
    new CSharpTdsClientPartial().run();
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
//        .option(TdsConnectionFactory.TRUST_SERVER_CERTIFICATE, false)
//        .option(TdsConnectionFactory.TRUST_STORE, "c:/users/karln/IdeaProjects/JavaTdsLibCopilot/myTrustStore.jks")
//        .option(TdsConnectionFactory.TRUST_STORE_PASSWORD, "changeit")
        .build());

    System.out.println("Connecting to database...");
    Publisher<? extends Connection> connectionPublisher = connectionFactory.create();

    CountDownLatch latch = new CountDownLatch(1);

    MappingProducer.from(connectionPublisher)
        .map(conn -> {
          try {
            runSql(conn);
            return "runSql executed successfully";
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        })
        .doOnComplete(latch::countDown)
        .doOnError(t -> {
          System.err.println("Connection/Run Failed: " + t.getMessage());
          latch.countDown();
        })
        .subscribe(System.out::println);

    latch.await();
  }

  @SuppressWarnings("JpaQueryApiInspection")
  private void runSql(Connection connection) throws InterruptedException {
//// 5. Select and Stream Clob
//    executeStream("5. Stream Clob", connection.createStatement(querySql).execute(), res ->
//        MappingProducer.from(res.map((row, meta) -> row.get(0, Clob.class)))
//            .flatMap(clob -> {
//              if (clob != null) {
//                return clob.stream(); // Open the valve! Returns Publisher<CharSequence>
//              }
//              return new EmptyPublisher<>();
//            })
//    );

// 5. Select and Stream Blob
    executeStream("5. Stream Blob", connection.createStatement(querySql).execute(), res ->
        MappingProducer.from(res.map((row, meta) -> row.get(0, Blob.class)))
            .flatMap(blob -> {
              if (blob != null) {
                return blob.stream(); // Open the valve! Returns Publisher<ByteBuffer>
              }
              return new EmptyPublisher<>();
            })
    );

    // Example of how a user would gracefully close it:
    MappingProducer.from(connection.close())
        .subscribe(new Subscriber<Void>() {
          @Override public void onSubscribe(Subscription s) { s.request(1); }
          @Override public void onNext(Void unused) {}
          @Override public void onError(Throwable t) { t.printStackTrace(); }
          @Override public void onComplete() {
            System.out.println("Connection safely closed.");
          }
        });
  }

  // --- The Universal Async Helper ---

  /**
   * Executes the publisher, maps the Result using the provided extractor function (e.g., res -> res.map(...)),
   * and strictly manages the Reactive Streams backpressure and Countdown Latch.
   */
  private <T> void executeStream(String stepName, Publisher<? extends Result> resultPublisher, Function<Result, Publisher<T>> extractor) throws InterruptedException {
    System.out.println("\n--- Executing: " + stepName + " ---");
    CountDownLatch latch = new CountDownLatch(1);

    MappingProducer.from(resultPublisher)
        .flatMap(res -> MappingProducer.from(extractor.apply(res)))
        .subscribe(new Subscriber<T>() {
          @Override
          public void onSubscribe(Subscription s) {
            s.request(Long.MAX_VALUE);
          }

          @Override
          public void onNext(T item) {
            System.out.println("  -> " + item);
          }

          @Override
          public void onError(Throwable t) {
            System.err.println("[" + stepName + "] Stream Error: " + t.getMessage());
            t.printStackTrace();
            latch.countDown();
          }

          @Override
          public void onComplete() {
            System.out.println("--- Completed: " + stepName + " ---");
            latch.countDown();
          }
        });

    latch.await();
  }

  static class EmptyPublisher<T> implements Publisher<T> {
    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
      subscriber.onSubscribe(new Subscription() {
        @Override public void request(long n) {}
        @Override public void cancel() {}
      });
      subscriber.onComplete();
    }
  }

  //  private static final String querySql = """
  //      SELECT test_varchar_max FROM dbo.AllDataTypes WHERE test_varchar_max IS NOT NULL;
  //      """;

  private static final String querySql = """
      SET TEXTSIZE -1;
      SELECT test_varbinary_max FROM dbo.AllDataTypes WHERE id = 1;
      """;

  //  BiFunction<Row, RowMetadata, AllDataTypesRecord> partialDataTypesMapper = (row, meta) -> new AllDataTypesRecord(
//      null,          null,
//      null,          null,
//      null,          null,
//      null,          null,
//      null,          null,
//      null,          null,
//      null,          null,
//      null,          null,
//      null,
//      null,
//      null,
//      null,
//      row.get(0, Clob.class),
//      null,
//      null,
//      null,
//      null,
//      null,
//      null,
//      null,
//      null,
//      null,
//      null
//  );
}