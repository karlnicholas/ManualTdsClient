package com.example;

// ... (Standard Imports & Setup Boilerplate) ...
import io.r2dbc.pool.*; import io.r2dbc.spi.*; import org.tdslib.javatdslib.api.*;
import reactor.core.publisher.*;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class TdsClientBindExceptions {
  public static void main(String[] args) { new TdsClientBindExceptions().run(); }

  private void run() {
    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(DRIVER, "javatdslib").option(HOST, "localhost").option(PORT, 1433)
        .option(USER, "reactnonreact").option(PASSWORD, "reactnonreact").option(DATABASE, "reactnonreact")
        .option(TdsLibOptions.TRUST_SERVER_CERTIFICATE, true).build())).initialSize(2).build());
    Mono.usingWhen(Mono.just(pool), this::runSql, ConnectionPool::disposeLater).block();
  }

  public Mono<Void> runSql(ConnectionPool pool) {
    return Mono.usingWhen(Mono.from(pool.create()),
        conn -> testCrashInRowMapper(conn)
            .then(testCrashInOutParameterExtraction(conn))
            .then(testSocketIntegrity(conn)),
        Connection::close);
  }

  /**
   * Bad Code 1: NullPointerException inside the Row mapper of a parameterized query.
   */
  private Mono<Void> testCrashInRowMapper(Connection connection) {
    System.out.println("\n--- Test 1: Exception Thrown Inside RPC Row Mapper ---");
    return Flux.from(connection.createStatement("SELECT @p1 AS val UNION ALL SELECT @p2")
            .bind("p1", 1).bind("p2", 2).execute())
        .flatMap(result -> result.map((row, meta) -> {
          throw new NullPointerException("Simulated NPE inside RPC row mapping!");
        }))
        .then()
        .onErrorResume(e -> {
          System.out.println(" Caught expected mapper crash: " + e.getMessage());
          return Mono.empty();
        });
  }

  /**
   * Bad Code 2: ClassCastException while extracting an OutSegment.
   * Proves the Graceful Discard handles trailing RPC tokens if a stream crashes at the very end.
   */
  private Mono<Void> testCrashInOutParameterExtraction(Connection connection) {
    System.out.println("\n--- Test 2: Blind Cast during OutSegment Extraction ---");
    return Flux.from(connection.createStatement("DECLARE @out INT = @in * 2; SELECT @out;")
            .bind("in", 10).execute()) // Note: Pretending this is a stored proc that returns an OutParam
        .flatMap(result -> result.flatMap(segment -> {
          // BAD CODE: Assuming every segment is a RowSegment, crashing on the UpdateCount or OutSegment
          Result.RowSegment badCast = (Result.RowSegment) segment;
          return Mono.just(badCast.row().get(0));
        }))
        .then()
        .onErrorResume(e -> {
          System.out.println(" Caught expected blind cast: " + e.getClass().getSimpleName());
          return Mono.empty();
        });
  }

  private Mono<Void> testSocketIntegrity(Connection conn) {
    return Flux.from(conn.createStatement("SELECT 99").execute()).flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class))).then();
  }
}