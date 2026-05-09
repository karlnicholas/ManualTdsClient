package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.api.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.Function;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class TdsClientStatementAddLobBinding {

  public static void main(String[] args) {
    new TdsClientStatementAddLobBinding().run();
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

    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(connectionFactory)
        .initialSize(1)
        .maxSize(5)
        .build());

    Mono.usingWhen(
        Mono.just(pool),
        this::runBatchTest,
        p -> p.disposeLater()
    ).block();
  }

  private Mono<Void> runBatchTest(ConnectionPool pool) {
    return Mono.usingWhen(
        Mono.from(pool.create()),
        connection -> {
          String setupSql = "DROP TABLE IF EXISTS dbo.BatchLobTest; " +
              "CREATE TABLE dbo.BatchLobTest (id INT, data VARBINARY(MAX));";

          return Mono.from(executeStream("Setup", connection.createStatement(setupSql).execute(), Result::getRowsUpdated))
              .then(executeBatchLob(connection));
        },
        Connection::close
    );
  }

  private Mono<Void> executeBatchLob(Connection connection) {
    String insertSql = "INSERT INTO dbo.BatchLobTest (id, data) VALUES (@id, @data)";

    // The Goldilocks Zone: 1000 chunks = ~8MB per LOB.
    // Heavy enough to instantly fill the OS socket buffer and trigger the 16-packet watermark,
    // but light enough to finish immediately.
    Publisher<ByteBuffer> heavyStream = Flux.range(0, 1000)
        .map(i -> {
          byte[] bytes = new byte[1024 * 8];
          Arrays.fill(bytes, i.byteValue());
          return ByteBuffer.wrap(bytes);
        });

    Statement stmt = connection.createStatement(insertSql);

    // Bind first set (StatementAdd 1)
    stmt.bind("@id", 1).bind("@data", Blob.from(heavyStream)).add();

    // Bind second set (StatementAdd 2)
    stmt.bind("@id", 2).bind("@data", Blob.from(heavyStream)).add();

    // Bind third set (Standard byte array to test mix)
    byte[] smallData = new byte[]{0x10, 0x20, 0x30};
    stmt.bind("@id", 3).bind("@data", ByteBuffer.wrap(smallData)).add();

    System.out.println("\n--- Executing Parameterized LOB StatementAdd ---");
    return Flux.from(stmt.execute())
        .flatMap(Result::getRowsUpdated)
        .doOnNext(count -> System.out.println("  -> StatementAdd part updated: " + count))
        .then();
  }

  private <T> Publisher<T> executeStream(String step, Publisher<? extends Result> pub, Function<Result, Publisher<T>> extractor) {
    return Flux.from(pub).flatMap(extractor);
  }
}