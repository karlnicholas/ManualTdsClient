package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class TdsClientTransactionExceptions {

  public static void main(String[] args) {
    new TdsClientTransactionExceptions().run();
  }

  private void run() {
    String r2dbcUrl = "r2dbc:mssql://reactnonreact:reactnonreact@localhost:1433/reactnonreact?trustServerCertificate=true";
    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(r2dbcUrl)).initialSize(2).maxSize(50)
.build());

    System.out.println("Connecting to database pool for Transaction Exception Testing...");

    Mono.usingWhen(Mono.just(pool), this::runSql, ConnectionPool::disposeLater)
        .doOnError(t -> System.err.println("\n❌ Test Suite Failed: " + t.getMessage()))
        .block();
  }

  public Mono<Void> runSql(ConnectionPool pool) {
    return Mono.usingWhen(Mono.from(pool.create()), this::runSql, Connection::close);
  }

  public Mono<Void> runSql(Connection connection) {
    return Mono.defer(() -> {
          System.out.println("\n--- Executing: 1. Setup ---");
          String setup = "DROP TABLE IF EXISTS dbo.TxExceptions; CREATE TABLE dbo.TxExceptions (id INT PRIMARY KEY);";
          return Flux.from(connection.createStatement(setup).execute()).flatMap(Result::getRowsUpdated).then();
        })

        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 2. Data Integrity Exception Mid-Transaction ---");
          return Mono.from(connection.beginTransaction())
              .thenMany(connection.createStatement("INSERT INTO dbo.TxExceptions (id) VALUES (1)").execute()) // Success
              .flatMap(Result::getRowsUpdated)
              .thenMany(connection.createStatement("INSERT INTO dbo.TxExceptions (id) VALUES (1)").execute()) // Fails (PK Violation)
              .flatMap(Result::getRowsUpdated)
              .then(Mono.<Void>error(new IllegalStateException("Expected constraint violation, but it succeeded!")))
              .onErrorResume(e -> {
                if (e instanceof IllegalStateException) return Mono.error(e);
                System.out.println("  ✓ [OK] Caught expected exception: " + e.getMessage());
                // The driver must allow us to gracefully rollback after an error state
                return Mono.from(connection.rollbackTransaction());
              });
        }))

        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 3. Double Begin Transaction ---");
          return Mono.from(connection.beginTransaction())
              .then(Mono.from(connection.beginTransaction())) // R2DBC spec says this shouldn't fail, but should be a no-op or throw IllegalStateException depending on strictness.
              .doOnSuccess(v -> System.out.println("  -> Driver ignored double begin. (Acceptable)"))
              .onErrorResume(e -> {
                System.out.println("  ✓ [OK] Driver strictly rejected double begin: " + e.getMessage());
                return Mono.empty();
              })
              .then(Mono.from(connection.rollbackTransaction()));
        }))

        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 4. Rollback Unknown Savepoint ---");
          return Mono.from(connection.beginTransaction())
              .then(Mono.from(connection.rollbackTransactionToSavepoint("NonExistentSavepoint")))
              .then(Mono.<Void>error(new IllegalStateException("Expected error on invalid savepoint, but succeeded!")))
              .onErrorResume(e -> {
                if (e instanceof IllegalStateException) return Mono.error(e);
                System.out.println("  ✓ [OK] Caught expected invalid savepoint error: " + e.getMessage());
                return Mono.from(connection.rollbackTransaction());
              });
        }));
  }
}