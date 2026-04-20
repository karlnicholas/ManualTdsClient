package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.*;
import org.tdslib.javatdslib.api.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.*;
import java.util.UUID;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class TdsClientBindingMatrixSymmetry {

  public static void main(String[] args) {
    new TdsClientBindingMatrixSymmetry().run();
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
    return setupTable(connection)
        .then(testWay1Positional(connection))
        .then(testWay2ExplicitR2dbcType(connection))
        .then(testWay3NamedNoAt(connection))
        .then(testWay4NamedWithAt(connection));
  }

  private Mono<Void> setupTable(Connection connection) {
    String ddl = "DROP TABLE IF EXISTS dbo.BindingSymmetry; " +
        "CREATE TABLE dbo.BindingSymmetry (" +
        "id INT IDENTITY(1,1) PRIMARY KEY, " +
        "val_bit BIT NULL, " +
        "val_int INT NULL, " +
        "val_bigint BIGINT NULL, " +
        "val_decimal DECIMAL(18,4) NULL, " +
        "val_dt2 DATETIME2 NULL, " +
        "val_dtoffset DATETIMEOFFSET NULL, " +
        "val_varchar VARCHAR(MAX) NULL" +
        ");";
    return Mono.from(connection.createStatement(ddl).execute()).then();
  }

  /**
   * Way 1: Positional Binding (0-based Index)
   * Testing symmetry with core Java types.
   */
  private Mono<Void> testWay1Positional(Connection connection) {
    System.out.println("\n--- Way 1: Positional Index Binding ---");
    Statement stmt = connection.createStatement(
        "INSERT INTO dbo.BindingSymmetry (val_bit, val_int, val_varchar) VALUES (@p0, @p1, @p2)");

    stmt.bind(0, true)                       // bit -> Boolean
        .bind(1, 2147483647)                 // int -> Integer
        .bind(2, "Index Based Binding");     // varchar -> String

    return Flux.from(stmt.execute()).flatMap(Result::getRowsUpdated).then();
  }

  /**
   * Way 2: Explicit R2dbcType Binding
   * Using Parameters.in() to ensure driver obeys the explicit type hint.
   */
  private Mono<Void> testWay2ExplicitR2dbcType(Connection connection) {
    System.out.println("\n--- Way 2: Explicit R2dbcType Binding ---");
    Statement stmt = connection.createStatement(
        "INSERT INTO dbo.BindingSymmetry (val_bigint, val_decimal, val_dt2) VALUES (@p0, @p1, @p2)");

    stmt.bind(0, Parameters.in(R2dbcType.BIGINT, 9223372036854775807L))
        .bind(1, Parameters.in(R2dbcType.DECIMAL, new BigDecimal("123.4567")))
        .bind(2, Parameters.in(R2dbcType.TIMESTAMP, LocalDateTime.now()));

    return Flux.from(stmt.execute()).flatMap(Result::getRowsUpdated).then();
  }

  /**
   * Way 3: Named Binding (No @ prefix)
   * Testing symmetry using ALTERNATIVE types from your decoder matrix.
   */
  private Mono<Void> testWay3NamedNoAt(Connection connection) {
    System.out.println("\n--- Way 3: Named Binding (No @) - Symmetry Testing ---");
    Statement stmt = connection.createStatement(
        "INSERT INTO dbo.BindingSymmetry (val_bit, val_int, val_decimal) VALUES (@myBit, @myInt, @myDec)");

    // Testing alternative mappings found in NumericDecoder convertSimple
    stmt.bind("myBit", (byte) 1)              // bit -> Byte
        .bind("myInt", (short) 32767)         // int -> Short
        .bind("myDec", new BigInteger("999")); // decimal -> BigInteger

    return Flux.from(stmt.execute()).flatMap(Result::getRowsUpdated).then();
  }

  /**
   * Way 4: Named Binding (With @ prefix)
   * Testing symmetry for temporal and complex types.
   */
  private Mono<Void> testWay4NamedWithAt(Connection connection) {
    System.out.println("\n--- Way 4: Named Binding (With @) - Symmetry Testing ---");
    Statement stmt = connection.createStatement(
        "INSERT INTO dbo.BindingSymmetry (val_dtoffset, val_varchar) VALUES (@atOffset, @atStr)");

    // Testing temporal symmetry from DateTimeDecoder
    stmt.bind("@atOffset", OffsetDateTime.now()) // datetimeoffset -> OffsetDateTime
        .bind("@atStr", "Named With At Binding");

    return Flux.from(stmt.execute()).flatMap(Result::getRowsUpdated).then();
  }
}