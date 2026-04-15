package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Row;
import org.tdslib.javatdslib.api.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsClientLobBug {

  public static void main(String[] args) {
    new TdsClientLobBug().run();
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

    // Configure a simple pool for the standalone client execution
    ConnectionPoolConfiguration poolConfiguration = ConnectionPoolConfiguration.builder(connectionFactory)
        .initialSize(2)
        .maxSize(10)
        .maxIdleTime(Duration.ofMinutes(10))
        .build();

    ConnectionPool pool = new ConnectionPool(poolConfiguration);

    System.out.println("Connecting to pool for Comprehensive Binding Matrix & Way Testing...");

    // Manage the Pool lifecycle and fail-fast on errors
    Mono.usingWhen(
            Mono.just(pool),
            this::runSql,
            p -> p.disposeLater().doOnSuccess(v -> System.out.println("\nTests complete. Connection pool closed."))
        )
        .doOnError(t -> System.err.println("\n❌ Test Suite Failed: " + t.getMessage()))
        .block();
  }

  /**
   * Overloaded method: Takes a ConnectionPool, borrows a single connection,
   * runs the tests, and safely releases the connection back to the pool.
   */
  public Mono<Void> runSql(ConnectionPool pool) {
    return Mono.usingWhen(
        Mono.from(pool.create()),
        this::runSql,
        Connection::close
    );
  }

  public Mono<Void> runSql(Connection connection) {
    // Query rearranged: Standard types first, LOB (_max) types last
    String sql = "SET TEXTSIZE -1; SELECT " +
        "test_varchar_max, test_nvarchar_max " +
        "FROM dbo.AllDataTypes WHERE id = 1;";

//    String sql = "SET TEXTSIZE -1; SELECT " +
//        "test_varchar_max, test_varbinary_max " +
//        "FROM dbo.AllDataTypes WHERE id = 1;";


    System.out.println("Executing Comprehensive Data Type Test...");

    return Flux.from(connection.createStatement(sql).execute())
        .flatMap(result -> result.map((row, meta) -> {
          System.out.println("--- Mapping All Standard Columns ---");

          return streamClob(row, "test_varchar_max").doOnNext(length -> System.out.println("  -> test_varchar_max length: " + length))
              .then(streamClob(row, "test_nvarchar_max").doOnNext(length -> System.out.println("  -> test_nvarchar_max length: " + length)));
        }))
        .flatMap(f -> f)
        .then();
  }

  // Helper to stream Clob and return length
  private Mono<Long> streamClob(Row row, String name) {
    Clob clob = row.get(name, Clob.class);
    if (clob == null) return Mono.just(0L);
    return Flux.from(clob.stream())
        .reduce(0L, (acc, chunk) -> {
          System.out.println("Acc, chunk.length() = " + acc + " : " + chunk.length());
          return acc + chunk.length();
        });
  }

}