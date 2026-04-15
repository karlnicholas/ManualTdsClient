package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.*;
import org.tdslib.javatdslib.api.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.time.*;
import java.util.Objects;
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

  // A simple record to hold the permutation data
  record NonNumericTestScenario(String columnName, Object javaValue, String description) {}

  public Mono<Void> runSql(Connection connection) {
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

    // Wrap execution in a transaction isolation boundary
    return Mono.from(connection.beginTransaction())
        .doOnSuccess(v -> System.out.println("  -> [BEGIN TRAN] Started test isolation boundary"))
        .then(scenarios.concatMap(scenario -> {

          // Use OUTPUT INSERTED for immediate visual validation
          String sql;
          if ("test_bit".equals(scenario.columnName())) {
            sql = "INSERT INTO dbo.AllDataTypes (test_bit) OUTPUT INSERTED.test_bit VALUES (@p0)";
          } else {
            sql = "INSERT INTO dbo.AllDataTypes (test_bit, " + scenario.columnName() +
                ") OUTPUT INSERTED." + scenario.columnName() + " VALUES (1, @p0)";
          }

          return Mono.from(connection.createStatement(sql)
                  .bind(0, scenario.javaValue())
                  .execute())
              .flatMap(result -> Mono.from(result.map((row, meta) -> row.get(0))))
              .doOnNext(retrievedValue -> {
                // Using deepEquals to safely compare byte[] arrays
                boolean matches = Objects.deepEquals(scenario.javaValue(), retrievedValue);
                if (matches) {
                  System.out.println("  ✓ PASS | " + scenario.description());
                } else {
                  System.out.println("  ✗ DIFF | " + scenario.description());
                  System.out.printf("      Sent:     [%s] %s%n", scenario.javaValue().getClass().getSimpleName(), scenario.javaValue());
                  System.out.printf("      Received: [%s] %s%n", retrievedValue != null ? retrievedValue.getClass().getSimpleName() : "null", retrievedValue);
                }
              })
              .doOnError(e -> System.err.println("  ✗ CRASH | " + scenario.description() + " -> " + e.getMessage()))
              .onErrorResume(e -> Mono.empty())
              .then();
        }).then())
        // Rollback to drop all inserted test data
        .then(Mono.from(connection.rollbackTransaction()))
        .doOnSuccess(v -> System.out.println("  -> [ROLLBACK TRAN] Cleaned up exhaustive non-numeric test data"));
  }
}