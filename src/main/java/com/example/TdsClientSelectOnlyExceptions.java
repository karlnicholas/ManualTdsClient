package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import org.tdslib.javatdslib.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsClientSelectOnlyExceptions {
  public static void main(String[] args) {
    new TdsClientSelectOnlyExceptions().run();
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

    ConnectionPoolConfiguration poolConfiguration = ConnectionPoolConfiguration.builder(connectionFactory)
        .initialSize(2)
        .maxSize(10)
        .maxIdleTime(Duration.ofMinutes(10))
        .build();

    ConnectionPool pool = new ConnectionPool(poolConfiguration);

    System.out.println("Connecting to pool for Client Exceptions Testing...");

    Mono.usingWhen(
            Mono.just(pool),
            this::runSql,
            p -> p.disposeLater().doOnSuccess(v -> System.out.println("\nTests complete. Connection pool closed."))
        )
        .doOnError(t -> System.err.println("\n❌ Test Suite Failed (Connection Dropped): " + t.getMessage()))
        .block();
  }

  public Mono<Void> runSql(ConnectionPool pool) {
    return Mono.usingWhen(
        Mono.from(pool.create()),
        connection ->
            testColumnNotFound(connection)
                .then(testTypeMismatch(connection))
                .then(testFilterBlindSegmentCast(connection))
                .then(testFlatMapReturnsNull(connection))
                .then(testMapNullViolation(connection))
                .then(testSocketIntegrity(connection, "Final Integrity Check")),
        Connection::close
    );
  }

  /**
   * Exception 1: IllegalArgumentException (Column Not Found)
   * The client asks for a column name or index that does not exist in the RowMetadata.
   */
  private Mono<Void> testColumnNotFound(Connection connection) {
    System.out.println("\n--- Test 1: Column Not Found ---");
    return Flux.from(connection.createStatement("SELECT 1 AS valid_col").execute())
        .flatMap(result -> result.map((row, meta) -> row.get("ghost_column", Integer.class)))
        .then()
        .onErrorResume(e -> {
          System.out.println(" Caught expected exception: " + e.getClass().getSimpleName() + " - " + e.getMessage());
          return Mono.empty();
        })
        .then(testSocketIntegrity(connection, "Post-Column Not Found Check"));
  }

  /**
   * Exception 2: ClassCastException / IllegalArgumentException (Type Mismatch)
   * The client attempts to map a database string to an incompatible Java type.
   */
  private Mono<Void> testTypeMismatch(Connection connection) {
    System.out.println("\n--- Test 2: Type Mismatch ---");
    return Flux.from(connection.createStatement("SELECT 'not_a_number' AS val").execute())
        .flatMap(result -> result.map((row, meta) -> row.get(0, Integer.class)))
        .then()
        .onErrorResume(e -> {
          System.out.println(" Caught expected exception: " + e.getClass().getSimpleName() + " - " + e.getMessage());
          return Mono.empty();
        })
        .then(testSocketIntegrity(connection, "Post-Type Mismatch Check"));
  }

  /**
   * Exception 3: ClassCastException (Blind Segment Casting in Filter)
   * The R2DBC 1.0 filter() method receives mixed segments. A poorly coded client
   * might assume every segment is a RowSegment, crashing when an UpdateCount arrives.
   */
  private Mono<Void> testFilterBlindSegmentCast(Connection connection) {
    System.out.println("\n--- Test 3: Blind Segment Cast in Filter ---");
    // This query generates an UpdateCount segment, not a Row segment.
    return Flux.from(connection.createStatement("PRINT 'This is a message segment';").execute())
        .flatMap(result -> result.filter(segment -> {
              // BAD CLIENT CODE: Blindly casting without instanceof check
              Result.RowSegment rowSegment = (Result.RowSegment) segment;
              return rowSegment.row() != null;
            })
            // The filtered Result MUST be mapped into a Publisher for Flux.flatMap to consume it
            .flatMap(segment -> Mono.empty()))
        .then()
        .onErrorResume(e -> {
          System.out.println(" Caught expected exception: " + e.getClass().getSimpleName() + " - " + e.getMessage());
          return Mono.empty();
        })
        .then(testSocketIntegrity(connection, "Post-Filter Cast Check"));
  }

  /**
   * Exception 4: NullPointerException (Reactive Spec Violation in FlatMap)
   * The flatMap function must return a Publisher. Returning null violates the Reactive Streams
   * specification and causes an immediate framework-level NPE.
   */
  private Mono<Void> testFlatMapReturnsNull(Connection connection) {
    System.out.println("\n--- Test 4: FlatMap Returns Null Publisher ---");
    return Flux.from(connection.createStatement("SELECT 1 AS val").execute())
        .flatMap(result -> result.flatMap(segment -> {
          // BAD CLIENT CODE: flatMap must return Mono.empty(), never null
          // Cast added to prevent Java compiler type-inference errors
          return (org.reactivestreams.Publisher<Void>) null;
        }))
        .then()
        .onErrorResume(e -> {
          System.out.println(" Caught expected exception: " + e.getClass().getSimpleName() + " - " + e.getMessage());
          return Mono.empty();
        })
        .then(testSocketIntegrity(connection, "Post-FlatMap Null Check"));
  }

  /**
   * Exception 5: NullPointerException (Reactive Spec Violation in Map)
   * The map function evaluates to null. Project Reactor operators cannot emit null values.
   */
  private Mono<Void> testMapNullViolation(Connection connection) {
    System.out.println("\n--- Test 5: Map Emits Null Value ---");
    return Flux.from(connection.createStatement("SELECT 1 AS val").execute())
        .flatMap(result -> result.map((row, meta) -> {
          // BAD CLIENT CODE: Returning null from a mapper in Reactor
          return null;
        }))
        .then()
        .onErrorResume(e -> {
          System.out.println(" Caught expected exception: " + e.getClass().getSimpleName() + " - " + e.getMessage());
          return Mono.empty();
        })
        .then(testSocketIntegrity(connection, "Post-Map Null Check"));
  }

  private Mono<Void> testSocketIntegrity(Connection connection, String phase) {
    return Mono.defer(() -> {
      System.out.println(" -> Running Integrity Check (" + phase + ")...");
      return Flux.from(connection.createStatement("SELECT 99 AS test_val;").execute())
          .flatMap(result -> result.map((row, meta) -> row.get(0, Integer.class)))
          .doOnNext(val -> {
            if (val == 99) {
              System.out.println("    [OK] Integrity Check Passed.");
            } else {
              System.err.println("    [FAIL] Received unexpected value: " + val);
            }
          })
          .then();
    });
  }
}