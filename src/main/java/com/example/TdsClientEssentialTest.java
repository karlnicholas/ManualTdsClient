package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.OutParameters;
import io.r2dbc.spi.OutParametersMetadata;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.example.AllDataTypesMapper.allDataTypesMapper;

public class TdsClientEssentialTest {
  public static void main(String[] args) {
    new TdsClientEssentialTest().run();
  }

  private void run() {
    String r2dbcUrl = "r2dbc:mssql://reactnonreact:reactnonreact@localhost:1433/reactnonreact?trustServerCertificate=true";
    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(r2dbcUrl)).initialSize(2).maxSize(50)
.build());

    System.out.println("Connecting to database pool for Transaction Testing...");

    UUID traceId = UUID.randomUUID();

    // Manage the Pool lifecycle
    Mono.usingWhen(
            Mono.just(pool),
            p -> runSql(p, traceId),
            p -> p.disposeLater().doOnSuccess(v -> System.out.println("\nConnection Pool safely closed."))
        )
        .doOnError(t -> System.err.println("\n❌ Test Suite Failed: " + t.getMessage()))
        .block();
  }

  /**
   * Overloaded method: Takes a ConnectionPool, borrows a single connection,
   * runs the tests, and releases it.
   */
  public Mono<Void> runSql(ConnectionPool pool, UUID traceId) {
    return Mono.usingWhen(
        Mono.from(pool.create()),
        conn -> runSql(conn, traceId), // Delegates to the existing logic
        Connection::close
    );
  }

  @SuppressWarnings("JpaQueryApiInspection")
  public Mono<Void> runSql(Connection connection, UUID traceId) {

    return
        Mono.defer(() -> executeStream("5. Select All", connection.createStatement(querySql).execute(), res -> res.map(allDataTypesMapper)))

        .then(Mono.defer(() -> executeStream("5. Select All Names", connection.createStatement(querySqlNames).execute(), res -> res.map(allDataTypesMapperNames))))

        .then(Mono.defer(() -> {
          String outSql = "SELECT @count = COUNT(*), @sum = SUM(postCount), @average = AVG(postCount) FROM dbo.users";
          Statement stmt6 = connection.createStatement(outSql)
              .bind("@count", Parameters.out(R2dbcType.BIGINT))
              .bind("@sum", Parameters.out(R2dbcType.BIGINT))
              .bind("@average", Parameters.out(R2dbcType.BIGINT));

          return executeStream("6. Select Out Parameters", stmt6.execute(), res -> res.flatMap(segment -> {
            if (segment instanceof Result.OutSegment outSeg) {
              OutParameters out = outSeg.outParameters();
              return Flux.just(rvOutMapper.apply(out, out.getMetadata()));
            }
            return Flux.empty();
          }));
        }))

        .then(Mono.defer(() -> executeStream("7. Update Row", connection.createStatement("UPDATE dbo.AllDataTypes SET test_bit = 0 WHERE id=1").execute(), Result::getRowsUpdated)))

        .then(Mono.defer(() -> executeStream("8. String Batch Execution", connection.createStatement(String.join("\n", batchSql)).execute(), res -> res.map(allDataTypesMapper))))

        .then(Mono.defer(() -> {
          Statement stmt9 = connection.createStatement("UPDATE dbo.AllDataTypes SET test_bit = 1 WHERE id = @id");
          stmt9.bind("@id", 1).add();
          stmt9.bind("@id", 2).add();
          stmt9.bind("@id", 3).add();
          return executeStream("9. Statement Parameter Batching", stmt9.execute(), Result::getRowsUpdated);
        }))

        .then(Mono.defer(() -> {
          Statement stmt9 = connection.createStatement("UPDATE dbo.AllDataTypes SET test_bit = 1 WHERE id = @id");

          // Omit the '@' in the bind string.
          // Call .add() BETWEEN bindings, but not on the final one.
          stmt9.bind("id", 1).add(); // Saves binding 1, prepares binding 2
          stmt9.bind("id", 2).add(); // Saves binding 2, prepares binding 3
          stmt9.bind("id", 3);       // Binds 3. NO .add() here!

          return executeStream("9b. Statement Parameter Batching", stmt9.execute(), Result::getRowsUpdated);
        }))

        .then(Mono.defer(() -> {
          Batch batch = connection.createBatch();
          batchSql.forEach(batch::add);
          return executeStream("10. createBatch() API", batch.execute(), res -> res.map(allDataTypesMapper));
        }))

        // Tests 11 & 12: Errors are strictly expected. If they don't error, the test fails.
        .then(Mono.defer(() -> executeExpectedErrorStream("11a. Runtime Error Test (Invalid Cast)", connection.createStatement("SELECT CAST('NotAnInteger' AS INT)").execute(), res -> res.map((row, meta) -> row.get(0, Integer.class)))))
        .then(Mono.defer(() -> executeExpectedErrorStream("11b. Runtime Error Test (RAISERROR)", connection.createStatement("RAISERROR('This is a fatal runtime exception', 16, 1)").execute(), Result::getRowsUpdated)))
        .then(Mono.defer(() -> executeExpectedErrorStream("12. Invalid Table Test", connection.createStatement("SELECT * FROM dbo.TableThatDoesNotExist").execute(), res -> res.map((row, meta) -> row.get(0, String.class)))))

        .contextWrite(Context.of("trace-id", traceId));
  }

  // --- Strict Async Helper ---
  // If an error happens here, it bubbles up and FAILS the test suite.
  private <T> Mono<Void> executeStream(String stepName, Publisher<? extends Result> resultPublisher, Function<Result, Publisher<T>> extractor) {
    System.out.println("\n--- Executing: " + stepName + " ---");

    return Flux.from(resultPublisher)
        .flatMap(extractor)
        .doOnNext(item -> System.out.println("  -> " + item))
        .doOnError(error -> System.err.println("[" + stepName + "] ❌ FAILED: " + error.getMessage()))
        .doOnComplete(() -> System.out.println("--- Completed: " + stepName + " ---"))
        .then();
  }

  // --- Expected Failure Helper ---
  // Fails the test if the database query accidentally succeeds.
  private <T> Mono<Void> executeExpectedErrorStream(String stepName, Publisher<? extends Result> resultPublisher, Function<Result, Publisher<T>> extractor) {
    System.out.println("\n--- Executing [EXPECTS ERROR]: " + stepName + " ---");

    return Flux.from(resultPublisher)
        .flatMap(extractor)
        .doOnNext(item -> System.out.println("  -> " + item))
        // If the stream completes normally, we throw an error because we EXPECTED a failure!
        .then(Mono.<Void>error(new IllegalStateException("Step '" + stepName + "' was expected to throw an error, but it succeeded!")))
        .onErrorResume(e -> {
          // If the error is OUR IllegalStateException, propagate it upstream to fail the suite
          if (e instanceof IllegalStateException) {
            return Mono.error(e);
          }
          // Otherwise, it's the expected database error. Catch it, log it, and complete successfully.
          System.out.println("  ✓ [Expected Database Error Caught]: " + e.getMessage());
          System.out.println("--- Completed: " + stepName + " ---");
          return Mono.empty();
        });
  }

  // --- Mappers & SQL Definitions ---

  private static final String bindSqlNames = """
    INSERT INTO dbo.AllDataTypes (
      test_bit, test_tinyint, test_smallint, test_int, test_bigint, test_decimal, test_numeric, 
      test_smallmoney, test_money, test_real, test_float, test_date, test_time, test_datetime, 
      test_datetime2, test_smalldatetime, test_dtoffset, test_char, test_varchar, test_varchar_max, 
      test_text, test_nchar, test_nvarchar, test_nvarchar_max, test_binary, test_varbinary, 
      test_varbinary_max, test_image, test_guid, test_xml
    ) VALUES (
  @test_bit, @test_tinyint, @test_smallint, @test_int, @test_bigint, @test_decimal, @test_numeric,
  @test_smallmoney, @test_money, @test_real, @test_float, @test_date, @test_time, @test_datetime,
  @test_datetime2, @test_smalldatetime, @test_dtoffset, @test_char, @test_varchar, @test_varchar_max,
  @test_text, @test_nchar, @test_nvarchar, @test_nvarchar_max, @test_binary, @test_varbinary,
  @test_varbinary_max, @test_image, @test_guid, @test_xml
    )""";

  private static final String querySqlNames = """
    SELECT id, 
      test_bit, test_tinyint, test_smallint, test_int, test_bigint, test_decimal, test_numeric, 
      test_smallmoney, test_money, test_real, test_float, test_date, test_time, test_datetime, 
      test_datetime2, test_smalldatetime, test_dtoffset, test_char, test_varchar, test_varchar_max, 
      test_text, test_nchar, test_nvarchar, test_nvarchar_max, test_binary, test_varbinary, 
      test_varbinary_max, test_image, test_guid, test_xml
    from dbo.AllDataTypes where id = 1;
""";

  BiFunction<Row, RowMetadata, AllDataTypesRecord> allDataTypesMapperNames = (row, meta) -> new AllDataTypesRecord(
      row.get("id", Integer.class),          row.get("test_bit", Boolean.class),
      row.get("test_tinyint", Byte.class),             row.get("test_smallint", Short.class),
      row.get("test_int", Integer.class),          row.get("test_bigint", Long.class),
      row.get("test_decimal", BigDecimal.class),       row.get("test_numeric", BigDecimal.class),
      row.get("test_smallmoney", BigDecimal.class),       row.get("test_money", BigDecimal.class),
      row.get("test_real", Float.class),           row.get("test_float", Double.class),
      row.get("test_date", LocalDate.class),       row.get("test_time", LocalTime.class),
      row.get("test_datetime", LocalDateTime.class),   row.get("test_datetime2", LocalDateTime.class),
      row.get("test_smalldatetime", LocalDateTime.class),   row.get("test_dtoffset", OffsetDateTime.class),
      row.get("test_char", String.class),
      row.get("test_varchar", String.class),
      row.get("test_varchar_max", String.class),          row.get("test_text", String.class),
      row.get("test_nchar", String.class),          row.get("test_nchar", String.class),
      row.get("test_nvarchar", String.class),          row.get("test_binary", byte[].class),
      row.get("test_varbinary", byte[].class),          row.get("test_varbinary_max", byte[].class),
      row.get("test_image", byte[].class),          row.get("test_guid", UUID.class),
      row.get("test_xml", String.class)
  );

  // Maps values out and actively queries OutParametersMetadata to print param names.
  BiFunction<OutParameters, OutParametersMetadata, List<String>> rvOutMapper = (out, meta) -> List.of(
      meta.getParameterMetadata(0).getName() + ": " + out.get(0, Long.class),
      meta.getParameterMetadata("@sum").getName() + ": " + out.get(1, Long.class),
      meta.getParameterMetadata(2).getName() + ": " + out.get(2, Long.class)
  );

  // --- SQL Definitions ---
  private static final String createSql = """
    DROP TABLE IF EXISTS dbo.AllDataTypes;
    CREATE TABLE dbo.AllDataTypes (
      id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
      test_bit BIT NOT NULL, test_tinyint TINYINT NULL, test_smallint SMALLINT NULL,
      test_int INT NULL, test_bigint BIGINT NULL, test_decimal DECIMAL(18, 4) NULL,
      test_numeric NUMERIC(10, 2) NULL, test_smallmoney SMALLMONEY NULL, test_money MONEY NULL,
      test_real REAL NULL, test_float FLOAT NULL, test_date DATE NULL, test_time TIME(7) NULL,
      test_datetime DATETIME NULL, test_datetime2 DATETIME2(7) NULL, test_smalldatetime SMALLDATETIME NULL,
      test_dtoffset DATETIMEOFFSET(7) NULL, test_char CHAR(10) NULL, test_varchar VARCHAR(50) NULL,
      test_varchar_max VARCHAR(MAX) NULL, test_text TEXT NULL, test_nchar NCHAR(10) NULL,
      test_nvarchar NVARCHAR(50) NULL, test_nvarchar_max NVARCHAR(MAX) NULL, test_binary BINARY(8) NULL,
      test_varbinary VARBINARY(50) NULL, test_varbinary_max VARBINARY(MAX) NULL, test_image IMAGE NULL,
      test_guid UNIQUEIDENTIFIER NULL, test_xml XML NULL
    );""";

  private static final String insertSql = """
    INSERT INTO dbo.AllDataTypes (
      test_bit, test_tinyint, test_smallint, test_int, test_bigint, test_decimal, test_numeric, 
      test_smallmoney, test_money, test_real, test_float, test_date, test_time, test_datetime, 
      test_datetime2, test_smalldatetime, test_dtoffset, test_char, test_varchar, test_varchar_max, 
      test_text, test_nchar, test_nvarchar, test_nvarchar_max, test_binary, test_varbinary, 
      test_varbinary_max, test_image, test_guid, test_xml
    ) VALUES (
      1, 255, 32000, 2000000000, 9000000000000000000, 12345.6789, 999.99, 214.99, 922337203685477.58,
      123.45, 123456789.987654321, '2023-12-25', '14:30:15.1234567', '2023-12-25 14:30:00',
      '2023-12-25 14:30:15.1234567', '2023-12-25 14:30:00', '2023-12-25 14:30:15.1234567 +05:30',
      'FixedChar', 'Euro: € and Cafe: Café', REPLICATE('A', 5000), 'Legacy Text Data', N'FixedUni',
      N'Unicode String', REPLICATE(N'あ', 4000), 0xDEADBEEF, 0xCAFEBABE, 0xFEEDBACC, 0x00112233, 
      NEWID(), '<root><node>Test XML</node></root>'
    );""";

  private static final String bindSql = """
    INSERT INTO dbo.AllDataTypes (
      test_bit, test_tinyint, test_smallint, test_int, test_bigint, test_decimal, test_numeric, 
      test_smallmoney, test_money, test_real, test_float, test_date, test_time, test_datetime, 
      test_datetime2, test_smalldatetime, test_dtoffset, test_char, test_varchar, test_varchar_max, 
      test_text, test_nchar, test_nvarchar, test_nvarchar_max, test_binary, test_varbinary, 
      test_varbinary_max, test_image, test_guid, test_xml
    ) VALUES (
      @p0, @p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12, @p13, @p14, @p15, 
      @p16, @p17, @p18, @p19, @p20, @p21, @p22, @p23, @p24, @p25, @p26, @p27, @p28, @p29
    )""";

  private static final String querySql = """
    SET TEXTSIZE -1;
    SELECT * FROM dbo.AllDataTypes where id = 1;
    """;

  private static final List<String> batchSql = List.of(
      "SET TEXTSIZE -1;",
      "SELECT * FROM dbo.AllDataTypes where id = 1;",
      "SELECT * FROM dbo.AllDataTypes where id = 2;",
      "SELECT * FROM dbo.AllDataTypes where id = 3;"
  );
}