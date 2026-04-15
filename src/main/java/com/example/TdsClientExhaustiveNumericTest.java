package com.example;

import io.r2dbc.spi.*;
import org.tdslib.javatdslib.api.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.BigInteger;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class TdsClientExhaustiveNumericTest {

  public static void main(String[] args) {
    new TdsClientExhaustiveNumericTest().run();
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

    Mono.usingWhen(
        Mono.from(connectionFactory.create()),
        this::runExhaustiveMatrix,
        conn -> Mono.from(conn.close()).doOnSuccess(v -> System.out.println("\nNumeric Matrix tests complete."))
    ).block();
  }

  // A simple record to hold the permutation data
  record NumericTestScenario(String columnName, Object javaValue, String description) {}

  private Mono<Void> runExhaustiveMatrix(Connection connection) {
    System.out.println("\n--- Running Exhaustive Numeric Matrix (Index-Based) ---");

    var scenarios = Flux.just(
        // --- BIT (7 permutations) ---
        new NumericTestScenario("test_bit", true, "Boolean to BIT"),
        new NumericTestScenario("test_bit", (byte) 1, "Byte to BIT"),
        new NumericTestScenario("test_bit", (short) 1, "Short to BIT"),
        new NumericTestScenario("test_bit", 1, "Integer to BIT"),
        new NumericTestScenario("test_bit", 1L, "Long to BIT"),
        new NumericTestScenario("test_bit", BigDecimal.ONE, "BigDecimal to BIT"),
        new NumericTestScenario("test_bit", BigInteger.ONE, "BigInteger to BIT"),

        // --- TINYINT (7 permutations) ---
        new NumericTestScenario("test_tinyint", (byte) 255, "Byte to TINYINT"),
        new NumericTestScenario("test_tinyint", true, "Boolean to TINYINT"),
        new NumericTestScenario("test_tinyint", (short) 255, "Short to TINYINT"),
        new NumericTestScenario("test_tinyint", 255, "Integer to TINYINT"),
        new NumericTestScenario("test_tinyint", 255L, "Long to TINYINT"),
        new NumericTestScenario("test_tinyint", new BigDecimal("255"), "BigDecimal to TINYINT"),
        new NumericTestScenario("test_tinyint", new BigInteger("255"), "BigInteger to TINYINT"),

        // --- SMALLINT (7 permutations) ---
        new NumericTestScenario("test_smallint", (short) 32767, "Short to SMALLINT"),
        new NumericTestScenario("test_smallint", true, "Boolean to SMALLINT"),
        new NumericTestScenario("test_smallint", (byte) 127, "Byte to SMALLINT"),
        new NumericTestScenario("test_smallint", 32767, "Integer to SMALLINT"),
        new NumericTestScenario("test_smallint", 32767L, "Long to SMALLINT"),
        new NumericTestScenario("test_smallint", new BigDecimal("32767"), "BigDecimal to SMALLINT"),
        new NumericTestScenario("test_smallint", new BigInteger("32767"), "BigInteger to SMALLINT"),

        // --- INT (7 permutations) ---
        new NumericTestScenario("test_int", 2147483647, "Integer to INT"),
        new NumericTestScenario("test_int", true, "Boolean to INT"),
        new NumericTestScenario("test_int", (byte) 127, "Byte to INT"),
        new NumericTestScenario("test_int", (short) 32767, "Short to INT"),
        new NumericTestScenario("test_int", 2147483647L, "Long to INT"),
        new NumericTestScenario("test_int", new BigDecimal("2147483647"), "BigDecimal to INT"),
        new NumericTestScenario("test_int", new BigInteger("2147483647"), "BigInteger to INT"),

        // --- BIGINT (7 permutations) ---
        new NumericTestScenario("test_bigint", 9223372036854775807L, "Long to BIGINT"),
        new NumericTestScenario("test_bigint", true, "Boolean to BIGINT"),
        new NumericTestScenario("test_bigint", (byte) 127, "Byte to BIGINT"),
        new NumericTestScenario("test_bigint", (short) 32767, "Short to BIGINT"),
        new NumericTestScenario("test_bigint", 2147483647, "Integer to BIGINT"),
        new NumericTestScenario("test_bigint", new BigDecimal("9223372036854775807"), "BigDecimal to BIGINT"),
        new NumericTestScenario("test_bigint", new BigInteger("9223372036854775807"), "BigInteger to BIGINT"),

        // --- REAL (2 permutations) ---
        new NumericTestScenario("test_real", 123.45f, "Float to REAL"),
        new NumericTestScenario("test_real", 123.45d, "Double to REAL"),

        // --- FLOAT (2 permutations) ---
        new NumericTestScenario("test_float", 123456789.987d, "Double to FLOAT"),
        new NumericTestScenario("test_float", 123.45f, "Float to FLOAT"),

        // --- DECIMAL (2 permutations) ---
        new NumericTestScenario("test_decimal", new BigDecimal("12345.6789"), "BigDecimal to DECIMAL"),
        new NumericTestScenario("test_decimal", new BigInteger("12345"), "BigInteger to DECIMAL"),

        // --- NUMERIC (2 permutations) ---
        new NumericTestScenario("test_numeric", new BigDecimal("999.99"), "BigDecimal to NUMERIC"),
        new NumericTestScenario("test_numeric", new BigInteger("999"), "BigInteger to NUMERIC"),

        // --- SMALLMONEY (1 permutation) ---
        new NumericTestScenario("test_smallmoney", new BigDecimal("214.99"), "BigDecimal to SMALLMONEY"),

        // --- MONEY (1 permutation) ---
        new NumericTestScenario("test_money", new BigDecimal("922337203685477.58"), "BigDecimal to MONEY")
    );

    // Process sequentially to keep logs readable
    return scenarios.concatMap(scenario -> {
// 1. The SQL Fix: Ensure test_bit is always populated to satisfy the NOT NULL constraint
      String sql;
      if ("test_bit".equals(scenario.columnName())) {
        sql = "INSERT INTO dbo.AllDataTypes (test_bit) VALUES (@p0)";
      } else {
        sql = "INSERT INTO dbo.AllDataTypes (test_bit, " + scenario.columnName() + ") VALUES (1, @p0)";
      }

      return Mono.from(connection.createStatement(sql)
              .bind(0, scenario.javaValue())
              .execute())
          .flatMapMany(Result::getRowsUpdated)
          .doOnNext(rows -> System.out.println("  ✓ " + scenario.description() + " -> Success"))
          .doOnError(e -> System.err.println("  ✗ " + scenario.description() + " -> FAILED: " + e.getMessage()))
          .onErrorResume(e -> Mono.empty()); // Swallow error to let the stream continue
    }).then();
  }
}