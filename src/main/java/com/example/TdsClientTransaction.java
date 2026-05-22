package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.Function;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class TdsClientTransaction {

  public static void main(String[] args) {
    new TdsClientTransaction().run();
  }

  private void run() {
    String r2dbcUrl = "r2dbc:mssql://reactnonreact:reactnonreact@localhost:1433/reactnonreact?trustServerCertificate=true";
    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(r2dbcUrl)).initialSize(2).maxSize(50)
.build());

    System.out.println("Connecting to database pool for Standard Transaction Testing...");

    Mono.usingWhen(Mono.just(pool), this::runSql, ConnectionPool::disposeLater)
        .doOnError(t -> System.err.println("\n❌ Test Suite Failed: " + t.getMessage()))
        .block();
  }

  public Mono<Void> runSql(ConnectionPool pool) {
    return Mono.usingWhen(Mono.from(pool.create()), this::runSql, Connection::close);
  }

  public Mono<Void> runSql(Connection connection) {
    return Mono.defer(() -> {
          System.out.println("\n--- Executing: 1. Setup DDL ---");
          String setup = "DROP TABLE IF EXISTS dbo.TxTest; CREATE TABLE dbo.TxTest (id INT IDENTITY(1,1), val VARCHAR(50));";
          return executeStream("1. Setup DDL", connection.createStatement(setup).execute(), Result::getRowsUpdated);
        })

        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 2. Successful Commit ---");
          return Mono.from(connection.beginTransaction())
              .thenMany(connection.createStatement("INSERT INTO dbo.TxTest (val) VALUES ('Commit_Me')").execute())
              .flatMap(Result::getRowsUpdated)
              .then(Mono.from(connection.commitTransaction()))
              .doOnSuccess(v -> System.out.println("  ✓ [OK] Transaction committed."));
        }))

        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 3. Successful Rollback ---");
          return Mono.from(connection.beginTransaction())
              .thenMany(connection.createStatement("INSERT INTO dbo.TxTest (val) VALUES ('Rollback_Me')").execute())
              .flatMap(Result::getRowsUpdated)
              .then(Mono.from(connection.rollbackTransaction()))
              .doOnSuccess(v -> System.out.println("  ✓ [OK] Transaction rolled back."));
        }))

    .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 4. Savepoint Rollback ---");
          return Mono.from(connection.beginTransaction())
              .thenMany(connection.createStatement("INSERT INTO dbo.TxTest (val) VALUES ('Keep_Me')").execute())
              .flatMap(Result::getRowsUpdated)
              .then(Mono.from(connection.createSavepoint("sp1")))
              .thenMany(connection.createStatement("INSERT INTO dbo.TxTest (val) VALUES ('Discard_Me')").execute())
              .flatMap(Result::getRowsUpdated)
              .then(Mono.from(connection.rollbackTransactionToSavepoint("sp1")))
              .then(Mono.from(connection.commitTransaction()))
              .doOnSuccess(v -> System.out.println("  ✓ [OK] Savepoint rolled back and transaction committed."));
        }))

    ;
  }

  private <T> Mono<Void> executeStream(String stepName, Publisher<? extends Result> resultPublisher, Function<Result, Publisher<T>> extractor) {
    return Flux.from(resultPublisher).flatMap(extractor).doOnNext(item -> System.out.println("  -> " + item)).then();
  }
}