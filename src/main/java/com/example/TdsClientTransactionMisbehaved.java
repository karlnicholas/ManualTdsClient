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

public class TdsClientTransactionMisbehaved {

  public static void main(String[] args) {
    new TdsClientTransactionMisbehaved().run();
  }

  private void run() {
    String r2dbcUrl = "r2dbc:mssql://reactnonreact:reactnonreact@localhost:1433/reactnonreact?trustServerCertificate=true";
    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(r2dbcUrl)).initialSize(2).maxSize(50)
.build());

    System.out.println("Connecting to database pool for Misbehaved Transaction Testing...");

    Mono.usingWhen(Mono.just(pool), this::runSql, ConnectionPool::disposeLater)
        .doOnError(t -> System.err.println("\n❌ Test Suite Failed: " + t.getMessage()))
        .block();
  }

  public Mono<Void> runSql(ConnectionPool pool) {
    return Mono.defer(() -> {
          System.out.println("\n--- Executing: 1. Setup ---");
          return Mono.usingWhen(Mono.from(pool.create()),
              conn -> Flux.from(conn.createStatement("DROP TABLE IF EXISTS dbo.MisbehavedTx; CREATE TABLE dbo.MisbehavedTx (val VARCHAR(50));").execute()).flatMap(Result::getRowsUpdated).then(),
              Connection::close);
        })

        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 2. Abandoned Transaction (Connection Closed Mid-Tx) ---");
          // Opens a transaction, inserts data, and CLOSES the connection without calling commit/rollback.
          // R2DBC pools rely on the driver to safely rollback uncommitted state when close() is called.
          return Mono.usingWhen(Mono.from(pool.create()),
              conn -> Mono.from(conn.beginTransaction())
                  .thenMany(conn.createStatement("INSERT INTO dbo.MisbehavedTx (val) VALUES ('Ghost Data')").execute())
                  .flatMap(Result::getRowsUpdated)
                  .doOnComplete(() -> System.out.println("  -> Inserted data inside uncommitted transaction. Closing connection...")).then(),
              Connection::close);
        }))

        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 3. Verify Pool Cleanup ---");
          // If the pool/driver failed to rollback the abandoned connection from Test 2, this will incorrectly find 'Ghost Data'
          return Mono.usingWhen(Mono.from(pool.create()),
              conn -> Flux.from(conn.createStatement("SELECT COUNT(*) FROM dbo.MisbehavedTx").execute())
                  .flatMap(res -> res.map((r, m) -> r.get(0, Integer.class)))
                  .doOnNext(count -> {
                    if (count > 0) throw new IllegalStateException("Pool leaked a transaction state! Found " + count + " ghost rows.");
                    System.out.println("  ✓ [OK] Connection pool successfully rolled back the abandoned transaction.");
                  }).then(),
              Connection::close);
        }))

        .then(Mono.defer(() -> {
          System.out.println("\n--- Executing: 4. Cancelled Commit Command ---");
          return Mono.usingWhen(Mono.from(pool.create()),
              conn -> Mono.from(conn.beginTransaction())
                  .thenMany(conn.createStatement("INSERT INTO dbo.MisbehavedTx (val) VALUES ('Cancelled Commit')").execute())
                  .flatMap(Result::getRowsUpdated)
                  .then(Mono.defer(() -> {
                    System.out.println("  -> Calling commitTransaction() but cancelling the flux immediately...");
                    // We subscribe to the commit but immediately cancel it via take(0) or timeout.
                    return Mono.from(conn.commitTransaction()).timeout(Duration.ofMillis(1)).onErrorResume(e -> Mono.empty());
                  })),
              Connection::close);
        }));
  }
}