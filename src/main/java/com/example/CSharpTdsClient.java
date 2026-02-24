package com.example;

import io.r2dbc.spi.*;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class CSharpTdsClient {
  public static void main(String[] args) throws Exception {
    new CSharpTdsClient().run();
  }

  private void run() throws Exception {
    ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(ConnectionFactoryOptions.DRIVER, "javatdslib")
        .option(HOST, "localhost")
        .option(PORT, 1433)
        .option(PASSWORD, "reactnonreact")
        .option(USER, "reactnonreact")
        .option(DATABASE, "reactnonreact")
        .build());

    System.out.println("Connecting to database...");
    Publisher<? extends Connection> connectionPublisher = connectionFactory.create();

    CountDownLatch latch = new CountDownLatch(1);

    MappingProducer.from(connectionPublisher)
        .map(conn -> {
          try {
            runSql(conn);
            return "runSql executed successfully";
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        })
        .doOnComplete(latch::countDown)
        .doOnError(t -> {
          System.err.println("Connection/Run Failed: " + t.getMessage());
          latch.countDown();
        })
        .subscribe(System.out::println);

    latch.await();
  }

  @SuppressWarnings("JpaQueryApiInspection")
  private void runSql(Connection connection) throws InterruptedException {
//    // 1. Create Table (DDL -> Update Count)
//    executeStream("1. Create Table", connection.createStatement(createSql).execute(), Result::getRowsUpdated);
//
//    // 2. Insert Initial Data (DML -> Update Count)
//    executeStream("2. Insert Initial Data", connection.createStatement(insertSql).execute(), Result::getRowsUpdated);
//
//    // 3. Large Parameterized Insert (R2dbcType wrapped)
//    Statement stmt3 = connection.createStatement(bindSql)
//        .bind(0, Parameters.in(R2dbcType.BOOLEAN, true))
//        .bind(1, Parameters.in(R2dbcType.TINYINT, (byte) 255))
//        .bind(2, Parameters.in(R2dbcType.SMALLINT, (short) 32000))
//        .bind(3, Parameters.in(R2dbcType.INTEGER, 2000000000))
//        .bind(4, Parameters.in(R2dbcType.BIGINT, 9000000000000000000L))
//        .bind(5, Parameters.in(R2dbcType.DECIMAL, new BigDecimal("12345.6789")))
//        .bind(6, Parameters.in(R2dbcType.NUMERIC, new BigDecimal("999.99")))
//        .bind(7, Parameters.in(R2dbcType.DECIMAL, new BigDecimal("214.99")))
//        .bind(8, Parameters.in(R2dbcType.DECIMAL, new BigDecimal("922337203685477.58")))
//        .bind(9, Parameters.in(R2dbcType.REAL, 123.45f))
//        .bind(10, Parameters.in(R2dbcType.DOUBLE, 123456789.987654321d))
//        .bind(11, Parameters.in(R2dbcType.DATE, LocalDate.of(2023, 12, 25)))
//        .bind(12, Parameters.in(R2dbcType.TIME, LocalTime.parse("14:30:15.1234567")))
//        .bind(13, Parameters.in(R2dbcType.TIMESTAMP, LocalDateTime.parse("2023-12-25T14:30:00")))
//        .bind(14, Parameters.in(R2dbcType.TIMESTAMP, LocalDateTime.parse("2023-12-25T14:30:15.1234567")))
//        .bind(15, Parameters.in(R2dbcType.TIMESTAMP, LocalDateTime.parse("2023-12-25T14:30:00")))
//        .bind(16, Parameters.in(R2dbcType.TIMESTAMP_WITH_TIME_ZONE, OffsetDateTime.parse("2023-12-25T14:30:15.1234567+05:30")))
//        .bind(17, Parameters.in(R2dbcType.CHAR, "FixedChar"))
//        .bind(18, Parameters.in(R2dbcType.VARCHAR, "Variable Length String"))
//        .bind(19, Parameters.in(R2dbcType.VARCHAR, "A".repeat(5000)))
//        .bind(20, Parameters.in(R2dbcType.VARCHAR, "Legacy Text Data"))
//        .bind(21, Parameters.in(R2dbcType.NCHAR, "FixedUni"))
//        .bind(22, Parameters.in(R2dbcType.NVARCHAR, "Unicode String"))
//        .bind(23, Parameters.in(R2dbcType.NVARCHAR, "あ".repeat(4000)))
//        .bind(24, Parameters.in(R2dbcType.BINARY, ByteBuffer.wrap(new byte[]{(byte)0xDE, (byte)0xAD, (byte)0xBE, (byte)0xEF})))
//        .bind(25, Parameters.in(R2dbcType.VARBINARY, ByteBuffer.wrap(new byte[]{(byte)0xCA, (byte)0xFE, (byte)0xBA, (byte)0xBE})))
//        .bind(26, Parameters.in(R2dbcType.VARBINARY, ByteBuffer.wrap(new byte[]{(byte)0xFE, (byte)0xED, (byte)0xBA, (byte)0xCC})))
//        .bind(27, Parameters.in(R2dbcType.VARBINARY, ByteBuffer.wrap(new byte[]{(byte)0x00, (byte)0x11, (byte)0x22, (byte)0x33})))
//        .bind(28, Parameters.in(R2dbcType.CHAR, UUID.randomUUID()))
//        .bind(29, Parameters.in(R2dbcType.NVARCHAR, "<root><node>Test XML</node></root>"));
//    executeStream("3. Parameterized Insert (R2dbcType)", stmt3.execute(), Result::getRowsUpdated);
//
//    // 4. Large Parameterized Insert (Inferred Types)
//    Statement stmt4 = connection.createStatement(bindSql)
//        .bind(0, true)
//        .bind(1, (byte)255)
//        .bind(2, (short) 32000)
//        .bind(3, 2000000000)
//        .bind(4, 9000000000000000000L)
//        .bind(5, new BigDecimal("12345.6789"))
//        .bind(6, new BigDecimal("999.99"))
//        .bind(7, new BigDecimal("214.99"))
//        .bind(8, new BigDecimal("922337203685477.58"))
//        .bind(9, 123.45f)
//        .bind(10, 123456789.987654321d)
//        .bind(11, LocalDate.of(2023, 12, 25))
//        .bind(12, LocalTime.parse("14:30:15.1234567"))
//        .bind(13, LocalDateTime.parse("2023-12-25T14:30:00"))
//        .bind(14, LocalDateTime.parse("2023-12-25T14:30:15.1234567"))
//        .bind(15, LocalDateTime.parse("2023-12-25T14:30:00"))
//        .bind(16, OffsetDateTime.parse("2023-12-25T14:30:15.1234567+05:30"))
//        .bind(17, "FixedChar")
//        .bind(18, "Euro: € and Cafe: Café")
//        .bind(19, "A".repeat(5000))
//        .bind(20, "Legacy Text Data")
//        .bind(21, "FixedUni")
//        .bind(22, "Unicode String")
//        .bind(23, "あ".repeat(4000))
//        .bind(24, ByteBuffer.wrap(new byte[]{(byte)0xDE, (byte)0xAD, (byte)0xBE, (byte)0xEF}))
//        .bind(25, ByteBuffer.wrap(new byte[]{(byte)0xCA, (byte)0xFE, (byte)0xBA, (byte)0xBE}))
//        .bind(26, ByteBuffer.wrap(new byte[]{(byte)0xFE, (byte)0xED, (byte)0xBA, (byte)0xCC}))
//        .bind(27, ByteBuffer.wrap(new byte[]{(byte)0x00, (byte)0x11, (byte)0x22, (byte)0x33}))
//        .bind(28, UUID.randomUUID())
//        .bind(29, "<root><node>Test XML</node></root>");
//    executeStream("4. Parameterized Insert (Inferred Types)", stmt4.execute(), Result::getRowsUpdated);
//
//    // 5. Select All (DQL -> Mapping)
//    executeStream("5. Select All", connection.createStatement(querySql).execute(), res -> res.map(allDataTypesMapper));
//
//    // 6. Select with Output Parameters
//    String outSql = "SELECT @count = COUNT(*), @sum = SUM(postCount), @average = AVG(postCount) FROM dbo.users";
//    Statement stmt6 = connection.createStatement(outSql)
//        .bind("@count", Parameters.out(R2dbcType.BIGINT))
//        .bind("@sum", Parameters.out(R2dbcType.BIGINT))
//        .bind("@average", Parameters.out(R2dbcType.BIGINT));
//    executeStream("6. Select Out Parameters", stmt6.execute(), res -> res.flatMap(segment -> {
//      if (segment instanceof Result.OutSegment) {
//        OutParameters out = ((Result.OutSegment) segment).outParameters();
//        return MappingProducer.just(rvOutMapper.apply(out, out.getMetadata()));
//      }
//      return new EmptyPublisher<>();
//    }));
//
//    // 7. Update single row
//    Statement stmt7 = connection.createStatement("UPDATE dbo.AllDataTypes SET test_bit = 0 WHERE id=1");
//    executeStream("7. Update Row", stmt7.execute(), Result::getRowsUpdated);
//
//    // 8. Select multiple batch from raw string
//    executeStream("8. String Batch Execution", connection.createStatement(String.join("\n", batchSql)).execute(), res -> res.map(allDataTypesMapper));
//
//    // 9. Statement.add() Parameter Batching (FIXED: Uses UPDATE instead of SELECT)
//    Statement stmt9 = connection.createStatement("UPDATE dbo.AllDataTypes SET test_bit = 1 WHERE id = @id");
//    stmt9.bind("@id", 1).add();
//    stmt9.bind("@id", 2).add();
//    stmt9.bind("@id", 3).add();
//    executeStream("9. Statement Parameter Batching", stmt9.execute(), Result::getRowsUpdated);
//
    // 10. True Batch API
    Batch batch = connection.createBatch();
    batchSql.forEach(batch::add);
    executeStream("10. createBatch() API", batch.execute(), res -> res.map(allDataTypesMapper));
    // 11. Example of how a user would gracefully close it:
    MappingProducer.from(connection.close())
        .subscribe(new Subscriber<Void>() {
          @Override public void onSubscribe(Subscription s) { s.request(1); }
          @Override public void onNext(Void unused) {}
          @Override public void onError(Throwable t) { t.printStackTrace(); }
          @Override public void onComplete() {
            System.out.println("Connection safely closed.");
          }
        });
  }

  // --- The Universal Async Helper ---

  /**
   * Executes the publisher, maps the Result using the provided extractor function (e.g., res -> res.map(...)),
   * and strictly manages the Reactive Streams backpressure and Countdown Latch.
   */
  private <T> void executeStream(String stepName, Publisher<? extends Result> resultPublisher, Function<Result, Publisher<T>> extractor) throws InterruptedException {
    System.out.println("\n--- Executing: " + stepName + " ---");
    CountDownLatch latch = new CountDownLatch(1);

    MappingProducer.from(resultPublisher)
        .flatMap(res -> MappingProducer.from(extractor.apply(res)))
        .subscribe(new Subscriber<T>() {
          @Override
          public void onSubscribe(Subscription s) {
            s.request(Long.MAX_VALUE);
          }

          @Override
          public void onNext(T item) {
            System.out.println("  -> " + item);
          }

          @Override
          public void onError(Throwable t) {
            System.err.println("[" + stepName + "] Stream Error: " + t.getMessage());
            t.printStackTrace();
            latch.countDown();
          }

          @Override
          public void onComplete() {
            System.out.println("--- Completed: " + stepName + " ---");
            latch.countDown();
          }
        });

    latch.await();
  }

  // --- Mappers ---

  BiFunction<Row, RowMetadata, AllDataTypesRecord> allDataTypesMapper = (row, meta) -> new AllDataTypesRecord(
      row.get(0, Integer.class),          row.get(1, Boolean.class),
      row.get(2, Byte.class),             row.get(3, Short.class),
      row.get(4, Integer.class),          row.get(5, Long.class),
      row.get(6, BigDecimal.class),       row.get(7, BigDecimal.class),
      row.get(8, BigDecimal.class),       row.get(9, BigDecimal.class),
      row.get(10, Float.class),           row.get(11, Double.class),
      row.get(12, LocalDate.class),       row.get(13, LocalTime.class),
      row.get(14, LocalDateTime.class),   row.get(15, LocalDateTime.class),
      row.get(16, LocalDateTime.class),   row.get(17, OffsetDateTime.class),
      row.get(18, String.class),
      row.get(19, String.class),
      row.get(20, String.class),          row.get(21, String.class),
      row.get(22, String.class),          row.get(23, String.class),
      row.get(24, String.class),          row.get(25, byte[].class),
      row.get(26, byte[].class),          row.get(27, byte[].class),
      row.get(28, byte[].class),          row.get(29, UUID.class),
      row.get(30, String.class)
  );

  BiFunction<OutParameters, OutParametersMetadata, List<Integer>> rvOutMapper = (out, meta) -> List.of(
      out.get(0, Integer.class),
      out.get(1, Integer.class),
      out.get(2, Integer.class)
  );

  // --- Utilities ---

  static class EmptyPublisher<T> implements Publisher<T> {
    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
      subscriber.onSubscribe(new Subscription() {
        @Override public void request(long n) {}
        @Override public void cancel() {}
      });
      subscriber.onComplete();
    }
  }

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
    SELECT * FROM dbo.AllDataTypes;
    """;

  private static final List<String> batchSql = List.of(
      "SET TEXTSIZE -1;",
      "SELECT * FROM dbo.AllDataTypes where id = 1;",
      "SELECT * FROM dbo.AllDataTypes where id = 2;",
      "SELECT * FROM dbo.AllDataTypes where id = 3;"
  );
}