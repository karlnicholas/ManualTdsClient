package com.example;

import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.tdslib.javatdslib.api.TdsConnectionFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class CSharpTdsClientRandom {
  public static void main(String[] args) {
    try {
      // .join() blocks the main thread until the entire chain is complete,
      // providing the same "wait" behavior as a latch but more cleanly.
      new CSharpTdsClientRandom().run().join();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private CompletableFuture<Void> run() {
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
    Publisher<? extends Connection> connectionPublisher = connectionFactory.create();

    // This future represents the "Life of the Program"
    CompletableFuture<Void> programLife = new CompletableFuture<>();

    MappingProducer.from(connectionPublisher)
        .map(conn -> {
          try {
            runSql(conn);
            return "runSql executed successfully";
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        })
        .doOnComplete(() -> programLife.complete(null))
        .doOnError(t -> {
          System.err.println("Connection/Run Failed: " + t.getMessage());
          programLife.completeExceptionally(t);
        })
        .subscribe(System.out::println);

    return programLife;
  }

  @SuppressWarnings("JpaQueryApiInspection")
  private void runSql(Connection connection) throws InterruptedException {
    executeStream("5. Diagnostic Blob Test", connection
            .createStatement(querySql)
            .execute(),
        res -> MappingProducer.from(res.map((row, meta) -> {
          System.out.println(">>> ROW HIT THE MAP FUNCTION!");
          Integer id = row.get(0, Integer.class);
//          Boolean bool = row.get(1, Boolean.class);
//          Byte tinyint = row.get(2, Byte.class);
//          Integer finalInt = row.get(3, Integer.class);
          return List.of(id);

          // Build the chain but BLOCK at the end of THIS row's mapping
          // This allows fragments to be read because the Event Loop
          // isn't blocked UNTIL the chain is fully constructed and joined.
//          try {
//            return consumeLOB(row.get(0, Clob.class).stream(), "CLOB1")
//                .thenCompose(v -> {
//                  Boolean bool = row.get(2, Boolean.class);
//                  return consumeLOB(row.get(3, Clob.class).stream(), "CLOB2");
//                })
//                .thenCompose(v -> {
//                  Byte tinyint = row.get(4, Byte.class);
//                  return consumeLOB(row.get(5, Blob.class).stream(), "BLOB");
//                })
//                .thenApply(v -> {
//                  Integer finalInt = row.get(6, Integer.class);
//                  return List.of("CLOB1");
//                }).get(); // <--- BLOCK HERE so the mapper returns the LIST, not the Future
//
//          } catch (Exception e) {
//            throw new RuntimeException(e);
//          }

        }))
    );
    // Simplified close logic
    MappingProducer.from(connection.close())
        .doOnComplete(() -> System.out.println("Connection safely closed."))
        .subscribe(unused -> {});
  }

  /**
   * Consumes a LOB stream asynchronously and returns a CompletableFuture
   * that completes when the LOB is fully drained.
   */
  private CompletableFuture<Void> consumeLOB(Publisher<?> lobStream, String label) {
    CompletableFuture<Void> future = new CompletableFuture<>();

    lobStream.subscribe(new DiagnosticSubscriber<>(label,
        item -> {
          // Processing logic (e.g., logging length)
          if (item instanceof ByteBuffer) {
            System.out.println(">>> " + label + ": onNext! Bytes: " + ((ByteBuffer)item).remaining());
          } else {
            System.out.println(">>> " + label + ": onNext! String Length: " + item.toString().length());
          }
        },
        () -> future.complete(null) // Signal completion to the chain
    ));

    return future;
  }
  /**
   * A reusable subscriber for diagnostic logging and latch management.
   */
  class DiagnosticSubscriber<T> implements Subscriber<T> {
    private final String label;
    private final Consumer<T> onNextAction;
    private final Runnable onCompleteAction;

    public DiagnosticSubscriber(String label, Consumer<T> onNextAction, Runnable onCompleteAction) {
      this.label = label;
      this.onNextAction = onNextAction;
      this.onCompleteAction = onCompleteAction;
    }

    @Override public void onSubscribe(Subscription s) {
      System.out.println(">>> " + label + ": onSubscribe. Requesting all...");
      s.request(Long.MAX_VALUE);
    }
    @Override public void onNext(T item) { onNextAction.accept(item); }
    @Override public void onError(Throwable t) { System.err.println(">>> " + label + " ERROR: " + t.getMessage()); }
    @Override public void onComplete() {
      System.out.println(">>> " + label + ": onComplete!");
      if (onCompleteAction != null) onCompleteAction.run();
    }
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

  private static final String querySql = """
      SET TEXTSIZE -1;
-- Alternating Standard and LOB columns
SELECT id FROM dbo.AllDataTypes;
""";


//  id,                -- (0) INT
//  test_nvarchar_max,  -- (1) CLOB
//  test_bit,          -- (2) BIT
//  test_nvarchar_max, -- (3) CLOB
//  test_tinyint,      -- (4) TINYINT
//  test_varbinary_max,-- (5) BLOB
//  test_int           -- (6) INT
}