package com.example;

import io.r2dbc.pool.*;
import io.r2dbc.spi.*;
import org.tdslib.r2dbc.mssql.*;
import reactor.core.publisher.*;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class TdsClientFilterExceptions {
  public static void main(String[] args) { new TdsClientFilterExceptions().run(); }

  private void run() {
    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(DRIVER, "javatdslib").option(HOST, "localhost").option(PORT, 1433)
        .option(USER, "reactnonreact").option(PASSWORD, "reactnonreact").option(DATABASE, "reactnonreact")
        .option(TdsLibOptions.TRUST_SERVER_CERTIFICATE, true).build())).initialSize(2).build());
    Mono.usingWhen(Mono.just(pool), this::runSql, ConnectionPool::disposeLater).block();
  }

  public Mono<Void> runSql(ConnectionPool pool) {
    return Mono.usingWhen(Mono.from(pool.create()),
        conn -> testPredicateThrowsException(conn)
            .then(testBlindSegmentCast(conn))
            .then(testSocketIntegrity(conn)),
        Connection::close);
  }

  /**
   * Bad Code 1: The user's Predicate function throws a RuntimeException.
   * The driver must safely catch this, abort the stream, and clear the socket.
   */
  private Mono<Void> testPredicateThrowsException(Connection connection) {
    System.out.println("\n--- Test 1: Exception Thrown Inside Predicate ---");
    return Flux.from(connection.createStatement("SELECT 1 AS val UNION ALL SELECT 2").execute())
        .map(result -> result.filter(segment -> {
          if (segment instanceof Result.RowSegment) {
            throw new NullPointerException("Simulated NPE inside user filter logic!");
          }
          return true;
        }))
        .flatMap(res -> res.flatMap(segment -> Mono.just(segment)))
        .then()
        .onErrorResume(e -> {
          System.out.println(" Caught expected predicate crash: " + e.getMessage());
          return Mono.empty();
        });
  }

  /**
   * Bad Code 2: Blind Casting.
   * The client blindly casts the segment to a RowSegment without checking instanceof.
   * Will crash on the DONE/UpdateCount token.
   */
  private Mono<Void> testBlindSegmentCast(Connection connection) {
    System.out.println("\n--- Test 2: Blind Segment Casting ---");
    return Flux.from(connection.createStatement("PRINT 'Hello'; SELECT 1 AS val;").execute())
        .map(result -> result.filter(segment -> {
          // BAD CODE: No instanceof check!
          Result.RowSegment rowSeg = (Result.RowSegment) segment;
          return rowSeg.row() != null;
        }))
        .flatMap(res -> res.flatMap(segment -> Mono.just(segment)))
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