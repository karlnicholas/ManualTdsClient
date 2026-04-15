package com.example;

import io.r2dbc.spi.*;
import org.tdslib.javatdslib.api.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.time.*;
import java.util.UUID;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class TdsClientExhaustiveNonNumericTest {

  public static void main(String[] args) {
    new TdsClientExhaustiveNonNumericTest().run();
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
        conn -> Mono.from(conn.close()).doOnSuccess(v -> System.out.println("\nNon-Numeric Matrix tests complete."))
    ).block();
  }

  // A simple record to hold the permutation data
  record NonNumericTestScenario(String columnName, Object javaValue, String description) {}

  private Mono<Void> runExhaustiveMatrix(Connection connection) {
    System.out.println("\n--- Running Exhaustive Non-Numeric Matrix (Index-Based) ---");

    byte[] sampleBytes = new byte[]{(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE};
    ByteBuffer sampleByteBuffer = ByteBuffer.wrap(sampleBytes);
    String sampleXml = "<root><element>Data</element></root>";
    UUID sampleUuid = UUID.randomUUID();

    var scenarios = Flux.just(
        // ================= TEMPORAL TYPES =================

        // --- DATE ---
        new NonNumericTestScenario("test_date", LocalDate.now(), "LocalDate to DATE"),

        // --- TIME ---
        new NonNumericTestScenario("test_time", LocalTime.now(), "LocalTime to TIME"),

        // --- DATETIME / DATETIME2 / SMALLDATETIME ---
        new NonNumericTestScenario("test_datetime", LocalDateTime.now(), "LocalDateTime to DATETIME"),
        new NonNumericTestScenario("test_datetime2", LocalDateTime.now(), "LocalDateTime to DATETIME2"),
        new NonNumericTestScenario("test_smalldatetime", LocalDateTime.now().withNano(0), "LocalDateTime to SMALLDATETIME"),

        // --- DATETIMEOFFSET (2 permutations) ---
        new NonNumericTestScenario("test_dtoffset", OffsetDateTime.now(), "OffsetDateTime to DATETIMEOFFSET"),
        new NonNumericTestScenario("test_dtoffset", ZonedDateTime.now(), "ZonedDateTime to DATETIMEOFFSET"),

        // ================= STRING TYPES =================

        // --- CHAR / VARCHAR ---
        new NonNumericTestScenario("test_char", "Fixed", "String to CHAR"),
        new NonNumericTestScenario("test_varchar", "Variable", "String to VARCHAR"),
        new NonNumericTestScenario("test_varchar_max", "A".repeat(5000), "Large String to VARCHAR(MAX)"),
        new NonNumericTestScenario("test_text", "Legacy Text", "String to TEXT"),

        // --- NCHAR / NVARCHAR (Unicode) ---
        new NonNumericTestScenario("test_nchar", "NFixed", "String to NCHAR"),
        new NonNumericTestScenario("test_nvarchar", "NVariable", "String to NVARCHAR"),
        new NonNumericTestScenario("test_nvarchar_max", "あ".repeat(4000), "Large Unicode String to NVARCHAR(MAX)"),

        // --- XML ---
        new NonNumericTestScenario("test_xml", sampleXml, "String to XML"),

        // --- UNIQUEIDENTIFIER (2 permutations) ---
        new NonNumericTestScenario("test_guid", sampleUuid, "UUID to UNIQUEIDENTIFIER"),
        new NonNumericTestScenario("test_guid", sampleUuid.toString(), "String to UNIQUEIDENTIFIER"),

        // ================= BINARY TYPES =================

        // --- BINARY (2 permutations) ---
        new NonNumericTestScenario("test_binary", sampleBytes, "byte[] to BINARY"),
        new NonNumericTestScenario("test_binary", sampleByteBuffer, "ByteBuffer to BINARY"),

        // --- VARBINARY (2 permutations) ---
        new NonNumericTestScenario("test_varbinary", sampleBytes, "byte[] to VARBINARY"),
        new NonNumericTestScenario("test_varbinary", sampleByteBuffer, "ByteBuffer to VARBINARY"),

        // --- VARBINARY(MAX) (2 permutations) ---
        new NonNumericTestScenario("test_varbinary_max", sampleBytes, "byte[] to VARBINARY(MAX)"),
        new NonNumericTestScenario("test_varbinary_max", sampleByteBuffer, "ByteBuffer to VARBINARY(MAX)"),

        // --- IMAGE (2 permutations) ---
        new NonNumericTestScenario("test_image", sampleBytes, "byte[] to IMAGE"),
        new NonNumericTestScenario("test_image", sampleByteBuffer, "ByteBuffer to IMAGE")
    );

    // Process sequentially to keep logs readable
    return scenarios.concatMap(scenario -> {

      // The SQL Fix: Ensure test_bit is always populated to satisfy the NOT NULL constraint
      String sql;
      if ("test_bit".equals(scenario.columnName())) {
        sql = "INSERT INTO dbo.AllDataTypes (test_bit) VALUES (@p0)";
      } else {
        sql = "INSERT INTO dbo.AllDataTypes (test_bit, " + scenario.columnName() + ") VALUES (1, @p0)";
      }

      // The Reactive Fix: Use Flux.from() and .flatMap() to ensure we intercept errors immediately
      return Flux.from(connection.createStatement(sql)
              .bind(0, scenario.javaValue())
              .execute())
          .flatMap(Result::getRowsUpdated)
          .doOnNext(rows -> System.out.println("  ✓ " + scenario.description() + " -> Success"))
          .doOnError(e -> System.err.println("  ✗ " + scenario.description() + " -> FAILED: " + e.getMessage()))
          .onErrorResume(e -> Mono.empty()); // Swallow error to let the stream continue
    }).then();
  }
}