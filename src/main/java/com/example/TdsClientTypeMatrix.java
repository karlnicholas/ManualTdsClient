package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.tdslib.javatdslib.api.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.UUID;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsClientTypeMatrix {
  public static void main(String[] args) throws Exception {
    new TdsClientTypeMatrix().run();
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
    System.out.println("Starting comprehensive type mapping tests...\n");

    // Chain all tests sequentially
    return testType(connection, "bit", "CAST(1 AS bit)", Boolean.class, Byte.class, Short.class, Integer.class, Long.class, BigDecimal.class, BigInteger.class)
        .then(testType(connection, "tinyint", "CAST(255 AS tinyint)", Byte.class, Boolean.class, Short.class, Integer.class, Long.class, BigDecimal.class, BigInteger.class))
        .then(testType(connection, "smallint", "CAST(32767 AS smallint)", Short.class, Boolean.class, Byte.class, Integer.class, Long.class, BigDecimal.class, BigInteger.class))
        .then(testType(connection, "int", "CAST(2147483647 AS int)", Integer.class, Boolean.class, Byte.class, Short.class, Long.class, BigDecimal.class, BigInteger.class))
        .then(testType(connection, "bigint", "CAST(9223372036854775807 AS bigint)", Long.class, Boolean.class, Byte.class, Short.class, Integer.class, BigDecimal.class, BigInteger.class))

        .then(testType(connection, "real", "CAST(123.45 AS real)", Float.class, Double.class))
        .then(testType(connection, "float", "CAST(123.456 AS float)", Double.class, Float.class))
        .then(testType(connection, "decimal", "CAST(123.45 AS decimal(5,2))", BigDecimal.class, BigInteger.class))
        .then(testType(connection, "numeric", "CAST(123.45 AS numeric(5,2))", BigDecimal.class, BigInteger.class))

        .then(testType(connection, "smallmoney", "CAST(123.45 AS smallmoney)", BigDecimal.class))
        .then(testType(connection, "money", "CAST(123.4567 AS money)", BigDecimal.class))

        .then(testType(connection, "uniqueidentifier", "CAST('12345678-1234-1234-1234-123456789012' AS uniqueidentifier)", UUID.class, String.class))

        .then(testType(connection, "smalldatetime", "CAST('2026-04-14T13:08:00' AS smalldatetime)", LocalDateTime.class))
        .then(testType(connection, "datetime", "CAST('2026-04-14T13:08:16.123' AS datetime)", LocalDateTime.class))
        .then(testType(connection, "datetime2", "CAST('2026-04-14T13:08:16.1234567' AS datetime2)", LocalDateTime.class))
        .then(testType(connection, "date", "CAST('2026-04-14' AS date)", LocalDate.class))
        .then(testType(connection, "time", "CAST('13:08:16.1234567' AS time)", LocalTime.class))
        .then(testType(connection, "datetimeoffset", "CAST('2026-04-14T13:08:16.1234567-04:00' AS datetimeoffset)", OffsetDateTime.class, ZonedDateTime.class))

        .then(testType(connection, "char", "CAST('A' AS char(1))", String.class))
        .then(testType(connection, "varchar", "CAST('Hello' AS varchar(5))", String.class))
        .then(testType(connection, "varcharmax", "CAST('Hello' AS varchar(max))", String.class))
        .then(testType(connection, "nchar", "CAST(N'A' AS nchar(1))", String.class))
        .then(testType(connection, "nvarchar", "CAST(N'Hello' AS nvarchar(5))", String.class))
        .then(testType(connection, "nvarcharmax", "CAST(N'Hello' AS nvarchar(max))", String.class))
        .then(testType(connection, "xml", "CAST('<r>v</r>' AS xml)", String.class))

        .then(testType(connection, "binary", "CAST(0x1234 AS binary(2))", ByteBuffer.class, byte[].class))
        .then(testType(connection, "varbinary", "CAST(0x1234 AS varbinary(2))", ByteBuffer.class, byte[].class))
        .then(testType(connection, "varbinarymax (ByteBuffer)", "CAST(0x1234 AS varbinary(max))", ByteBuffer.class))
        .then(testType(connection, "varbinarymax (byte[])", "CAST(0x1234 AS varbinary(max))", byte[].class))

        .then(testLegacyTypes(connection));
  }

  /**
   * Helper to execute a query and extract multiple Java types from the single returned column.
   */
  private Mono<Void> testType(Connection connection, String typeName, String sqlSelectCast, Class<?>... classesToTest) {
    String sql = "SELECT " + sqlSelectCast;
    return Flux.from(connection.createStatement(sql).execute())
        .flatMap(result -> result.map((row, meta) -> {
          System.out.println("--- Testing " + typeName + " ---");
          for (Class<?> clazz : classesToTest) {
            try {
              Object val = row.get(0, clazz);
              System.out.println("  ✓ Decoded as " + clazz.getSimpleName() + ": " + formatOutput(val));
            } catch (Exception e) {
              System.err.println("  ✗ FAILED decoding as " + clazz.getSimpleName() + ": " + e.getMessage());
            }
          }
          return "OK";
        }))
        .then();
  }

  /**
   * Legacy types (text, ntext, image, timestamp) cannot be constructed via direct CAST in SQL Server.
   * We test them using a temporary table variable.
   */
  private Mono<Void> testLegacyTypes(Connection connection) {
    String legacySql = """
        DECLARE @tmp TABLE (
            t_col text,
            nt_col ntext,
            i_col image,
            ts_col timestamp
        );
        INSERT INTO @tmp (t_col, nt_col, i_col) VALUES ('legacy text', N'legacy ntext', 0xDEADBEEF);
        SELECT t_col, nt_col, i_col, ts_col FROM @tmp;
        """;

    return Flux.from(connection.createStatement(legacySql).execute())
        .flatMap(result -> result.map((row, meta) -> {
          System.out.println("--- Testing Legacy Types ---");

          try {
            System.out.println("  [text] ✓ Decoded as String: " + row.get("t_col", String.class));
            System.out.println("  [ntext] ✓ Decoded as String: " + row.get("nt_col", String.class));

            System.out.println("  [image] ✓ Decoded as ByteBuffer: " + formatOutput(row.get("i_col", ByteBuffer.class)));
            System.out.println("  [image] ✓ Decoded as byte[]: " + formatOutput(row.get("i_col", byte[].class)));

            System.out.println("  [timestamp] ✓ Decoded as byte[]: " + formatOutput(row.get("ts_col", byte[].class)));
          } catch (Exception e) {
            System.err.println("  ✗ FAILED legacy type decoding: " + e.getMessage());
          }
          return "OK";
        }))
        .then();
  }

  /**
   * Safe formatter for byte arrays and buffers to avoid printing raw memory references
   */
  private String formatOutput(Object obj) {
    if (obj instanceof byte[] bytes) {
      StringBuilder sb = new StringBuilder("0x");
      for (byte b : bytes) {
        sb.append(String.format("%02X", b));
      }
      return sb.toString() + " (byte array)";
    }
    if (obj instanceof ByteBuffer buffer) {
      return "ByteBuffer[capacity=" + buffer.capacity() + "]";
    }
    return String.valueOf(obj);
  }
}