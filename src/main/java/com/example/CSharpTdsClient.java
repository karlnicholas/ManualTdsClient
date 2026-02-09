package com.example;

import io.r2dbc.spi.*;
import org.reactivestreams.Publisher;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;


public class CSharpTdsClient {
    public static void main(String[] args) throws Exception {
        new CSharpTdsClient().run();
    }

    private void run() throws Exception {
//        TdsConnectionImpl client = new TdsConnectionImpl(hostname, port);
//          client.connect("localhost", "reactnonreact", "reactnonreact", "reactnonreact", "app", "MyServerName", "us_english");
//            queryAsync("SELECT 1; SELECT 2;", client);
//            queryAsync("SELECT @@Version", client);
      ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
              .option(ConnectionFactoryOptions.DRIVER, "javatdslib")
              .option(HOST, "localhost")
              .option(PORT, 1433)
              .option(PASSWORD, "reactnonreact")
              .option(USER, "reactnonreact")
              .option(DATABASE, "reactnonreact")
              .build());

      System.out.println("Connecting to database... (connectionFactory)");
      Publisher<? extends Connection> connectionPublisher = connectionFactory.create();

      System.out.println("Connecting to database... (connectionPublisher)");

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
              .subscribe(
                      System.out::println,
                      throwable -> {
                        System.out.println("Error: " + throwable.getMessage());
                        latch.countDown();
                      },
                      latch::countDown
              );
      latch.await();
    }

  @SuppressWarnings("JpaQueryApiInspection")
  private void runSql(Connection connection) throws InterruptedException {
    asyncQueryFlatMap(createSql, connection, allDataTypesMapper);
    asyncQueryFlatMap(insertSql, connection, allDataTypesMapper);

//          sql = """
//    SET TEXTSIZE -1; -- Disable the 4096 byte limit
//    SELECT * FROM dbo.AllDataTypes;
//    """;
//          asyncQueryFlatMap(sql, connection, allDataTypesMapper);
    //
//              .bind("test_xml", io.r2dbc.mssql.MssqlType.XML.of("<root><node>Test XML</node></root>"));
    // Or simply bind a String for XML if the driver version supports implicit conversion

    Statement statement = connection.createStatement(bindSql)
            // --- Exact Numerics ---
            .bind(0, Parameters.in(R2dbcType.BOOLEAN, true))                        // test_bit
            .bind(1, Parameters.in(R2dbcType.TINYINT, (byte) 255))                  // test_tinyint
            .bind(2, Parameters.in(R2dbcType.SMALLINT, (short) 32000))              // test_smallint
            .bind(3, Parameters.in(R2dbcType.INTEGER, 2000000000))                  // test_int
            .bind(4, Parameters.in(R2dbcType.BIGINT, 9000000000000000000L))         // test_bigint
            .bind(5, Parameters.in(R2dbcType.DECIMAL, new BigDecimal("12345.6789")))// test_decimal
            .bind(6, Parameters.in(R2dbcType.NUMERIC, new BigDecimal("999.99")))    // test_numeric
            .bind(7, Parameters.in(R2dbcType.DECIMAL, new BigDecimal("214.99")))    // test_smallmoney
            .bind(8, Parameters.in(R2dbcType.DECIMAL, new BigDecimal("922337203685477.58"))) // test_money

            // --- Approximate Numerics ---
            .bind(9, Parameters.in(R2dbcType.REAL, 123.45f))                        // test_real
            .bind(10, Parameters.in(R2dbcType.DOUBLE, 123456789.987654321d))        // test_float

            // --- Date and Time ---
            .bind(11, Parameters.in(R2dbcType.DATE, LocalDate.of(2023, 12, 25)))    // test_date
            .bind(12, Parameters.in(R2dbcType.TIME, LocalTime.parse("14:30:15.1234567"))) // test_time
            .bind(13, Parameters.in(R2dbcType.TIMESTAMP, LocalDateTime.parse("2023-12-25T14:30:00"))) // test_datetime
            .bind(14, Parameters.in(R2dbcType.TIMESTAMP, LocalDateTime.parse("2023-12-25T14:30:15.1234567"))) // test_datetime2
            .bind(15, Parameters.in(R2dbcType.TIMESTAMP, LocalDateTime.parse("2023-12-25T14:30:00"))) // test_smalldatetime
            .bind(16, Parameters.in(R2dbcType.TIMESTAMP_WITH_TIME_ZONE, OffsetDateTime.parse("2023-12-25T14:30:15.1234567+05:30"))) // test_dtoffset

            // --- Character Strings ---
            .bind(17, Parameters.in(R2dbcType.CHAR, "FixedChar"))                   // test_char
            .bind(18, Parameters.in(R2dbcType.VARCHAR, "Variable Length String"))   // test_varchar
            .bind(19, Parameters.in(R2dbcType.VARCHAR, "A".repeat(5000)))           // test_varchar_max
            .bind(20, Parameters.in(R2dbcType.VARCHAR, "Legacy Text Data"))         // test_text

            // --- Unicode Strings ---
            .bind(21, Parameters.in(R2dbcType.NCHAR, "FixedUni"))                   // test_nchar
            .bind(22, Parameters.in(R2dbcType.NVARCHAR, "Unicode String"))          // test_nvarchar
            .bind(23, Parameters.in(R2dbcType.NVARCHAR, "あ".repeat(4000)))         // test_nvarchar_max

            // --- Binary Strings ---
            .bind(24, Parameters.in(R2dbcType.BINARY, ByteBuffer.wrap(new byte[]{(byte)0xDE, (byte)0xAD, (byte)0xBE, (byte)0xEF}))) // test_binary
            .bind(25, Parameters.in(R2dbcType.VARBINARY, ByteBuffer.wrap(new byte[]{(byte)0xCA, (byte)0xFE, (byte)0xBA, (byte)0xBE}))) // test_varbinary
            .bind(26, Parameters.in(R2dbcType.VARBINARY, ByteBuffer.wrap(new byte[]{(byte)0xFE, (byte)0xED, (byte)0xBA, (byte)0xCC}))) // test_varbinary_max
            .bind(27, Parameters.in(R2dbcType.VARBINARY, ByteBuffer.wrap(new byte[]{(byte)0x00, (byte)0x11, (byte)0x22, (byte)0x33}))) // test_image

            // --- Other Data Types ---
            .bind(28, Parameters.in(R2dbcType.CHAR, UUID.randomUUID()))             // test_guid (Standard R2dbcType fallback)
            .bind(29, Parameters.in(R2dbcType.NVARCHAR, "<root><node>Test XML</node></root>")); // test_xml

    rpcAsyncQueryFlapMap(statement, allDataTypesMapper);

    statement = connection.createStatement(bindSql)
            // --- Exact Numerics ---
            .bind(0, true)                                      // test_bit
            .bind(1, (byte)255)                                      // test_tinyint
            .bind(2, (short) 32000)                             // test_smallint
            .bind(3, 2000000000)                                // test_int
            .bind(4, 9000000000000000000L)                      // test_bigint
            .bind(5, new BigDecimal("12345.6789"))              // test_decimal
            .bind(6, new BigDecimal("999.99"))                  // test_numeric
            .bind(7, new BigDecimal("214.99"))                  // test_smallmoney
            .bind(8, new BigDecimal("922337203685477.58"))      // test_money

            // --- Approximate Numerics ---
            .bind(9, 123.45f)                                   // test_real
            .bind(10, 123456789.987654321d)                     // test_float

            // --- Date and Time ---
            .bind(11, LocalDate.of(2023, 12, 25))               // test_date
            .bind(12, LocalTime.parse("14:30:15.1234567"))      // test_time
            .bind(13, LocalDateTime.parse("2023-12-25T14:30:00"))         // test_datetime
            .bind(14, LocalDateTime.parse("2023-12-25T14:30:15.1234567")) // test_datetime2
            .bind(15, LocalDateTime.parse("2023-12-25T14:30:00"))         // test_smalldatetime
            .bind(16, OffsetDateTime.parse("2023-12-25T14:30:15.1234567+05:30")) // test_dtoffset

            // --- Character Strings ---
            .bind(17, "FixedChar")                              // test_char
            .bind(18, "Variable Length String")                 // test_varchar
            .bind(19, "A".repeat(5000))                         // test_varchar_max
            .bind(20, "Legacy Text Data")                       // test_text

            // --- Unicode Strings ---
            .bind(21, "FixedUni")                               // test_nchar
            .bind(22, "Unicode String")                         // test_nvarchar
            .bind(23, "あ".repeat(4000))                        // test_nvarchar_max

            // --- Binary Strings ---
            .bind(24, ByteBuffer.wrap(new byte[]{(byte)0xDE, (byte)0xAD, (byte)0xBE, (byte)0xEF})) // test_binary
            .bind(25, ByteBuffer.wrap(new byte[]{(byte)0xCA, (byte)0xFE, (byte)0xBA, (byte)0xBE})) // test_varbinary
            .bind(26, ByteBuffer.wrap(new byte[]{(byte)0xFE, (byte)0xED, (byte)0xBA, (byte)0xCC})) // test_varbinary_max
            .bind(27, ByteBuffer.wrap(new byte[]{(byte)0x00, (byte)0x11, (byte)0x22, (byte)0x33})) // test_image

            // --- Other Data Types ---
            .bind(28, UUID.randomUUID())                        // test_guid
            .bind(29, "<root><node>Test XML</node></root>");    // test_xml

    rpcAsyncQueryFlapMap(statement, allDataTypesMapper);

    asyncQueryFlatMap(querySql, connection, allDataTypesMapper);
//            sql = "INSERT INTO dbo.users (firstName, lastName, email, postCount) VALUES (@p1, @p2, @p3, @p4)";

//          Statement statement = connection.queryRpc(sql)
//                .bind("@p1", "Michael")
//                .bind("@p2", "Thomas")
//                .bind("@p3", "mt@mt.com")
//                .bind("@p4", 120L);
//
//          rpcAsyncQueryFlapMap(statement, longMapper);
//
////          SELECT @retval = COUNT(*) FROM dbo.users WHERE postCount > @p1
//            String sql = """
//                SELECT COUNT(*) FROM dbo.users WHERE postCount > @p1
//                """;
//          Statement statement = connection.queryRpc(sql)
//              .bind("@p1", 100L);
//          rpcAsyncQueryFlapMap(statement, longMapper);
//
//            sql = """
//                SELECT * FROM dbo.users WHERE postCount > @p1
//                """;
//            statement = connection.queryRpc(sql)
//                .bind("@p1", 100L);
//
//            rpcAsyncQueryFlapMap(statement, dbRecordMapper);
//
//          sql = """
//                SELECT * FROM dbo.users WHERE postCount > @p1
//                """;
//          statement = connection.queryRpc(sql)
//              .bind(1, 100L);
//
//          rpcAsyncQueryFlapMap(statement, dbRecordMapper);
//
    //            CountDownLatch latch = new CountDownLatch(1);
//            // 1. The Source: A publisher of R2DBC Results
//            statement.execute()            // If no error token was received, and SQL server did not close the connection, then the connection to the server is now established and the user is logged in.
  }

  BiFunction<Row, RowMetadata, DbRecord> dbRecordMapper =  (row, meta) -> new DbRecord(
          row.get(0, Long.class),
          row.get(1, String.class),
          row.get(2, String.class),
          row.get(3, String.class),
          row.get(4, LocalDate.class),
          row.get(5, Long.class),
          row.get(6, LocalDateTime.class),
          row.get(7, LocalDateTime.class)
  );

  BiFunction<Row, RowMetadata, AllDataTypesRecord> allDataTypesMapper = (row, meta) -> new AllDataTypesRecord(
          row.get(0, Integer.class),          // id

          // Exact Numerics
          row.get(1, Boolean.class),          // test_bit
          row.get(2, Byte.class),            // test_tinyint
          row.get(3, Short.class),            // test_smallint
          row.get(4, Integer.class),          // test_int
          row.get(5, Long.class),             // test_bigint
          row.get(6, BigDecimal.class),       // test_decimal
          row.get(7, BigDecimal.class),       // test_numeric
          row.get(8, BigDecimal.class),       // test_smallmoney
          row.get(9, BigDecimal.class),       // test_money

          // Approximate Numerics
          row.get(10, Float.class),           // test_real
          row.get(11, Double.class),          // test_float

          // Date and Time
          row.get(12, LocalDate.class),       // test_date
          row.get(13, LocalTime.class),       // test_time
          row.get(14, LocalDateTime.class),   // test_datetime
          row.get(15, LocalDateTime.class),   // test_datetime2
          row.get(16, LocalDateTime.class),   // test_smalldatetime
          row.get(17, OffsetDateTime.class),  // test_dtoffset

          // Character Strings
          row.get(18, String.class),          // test_char
          row.get(19, String.class),          // test_varchar
          row.get(20, String.class),          // test_varchar_max
          row.get(21, String.class),          // test_text

          // Unicode Strings
          row.get(22, String.class),          // test_nchar
          row.get(23, String.class),          // test_nvarchar
          row.get(24, String.class),          // test_nvarchar_max

          // Binary Strings
          row.get(25, byte[].class),          // test_binary
          row.get(26, byte[].class),          // test_varbinary
          row.get(27, byte[].class),          // test_varbinary_max
          row.get(28, byte[].class),          // test_image

          // Other
          row.get(29, UUID.class),            // test_guid
          row.get(30, String.class)           // test_xml
  );

  BiFunction<Row, RowMetadata, Long> longMapper =  (row, meta) -> row.get(0, Long.class);

  //  private final BiFunction<Row, RowMetadata, SomeEntity> mapper = (row, meta) -> {
//    SomeEntity someEntity = new SomeEntity();
//    someEntity.setId(row.get("id", Long.class));
//    someEntity.setSvalue(row.get("svalue", String.class));
//    return someEntity;
//  };

  private <T> void asyncQueryFlatMap(String sql, Connection connection, BiFunction<Row, RowMetadata, T> mapper) throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    MappingProducer.from(connection.createStatement(sql).execute())
            .flatMap(result -> result.map(mapper))
            .subscribe(
                    System.out::println,
                    throwable -> {
                      System.out.println("Error: " + throwable.getMessage());
                      latch.countDown();
                    },
                    latch::countDown
            );
    latch.await();
  }

  private <T> void rpcAsyncQueryFlapMap(Statement statement, BiFunction<Row, RowMetadata, T> mapper) throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    MappingProducer.from(statement.execute())
            .flatMap(result -> result.map(mapper))
            .subscribe(
                    System.out::println,
                    throwable -> {
                      System.out.println("Error: " + throwable.getMessage());
                      latch.countDown();
                    },
                    latch::countDown
            );
    latch.await();
  }
  private static final String createSql = """
DROP TABLE IF EXISTS dbo.AllDataTypes;

CREATE TABLE dbo.AllDataTypes (
-- Identity / Primary Key
id                  INT IDENTITY(1,1) NOT NULL PRIMARY KEY,

-- Exact Numerics
test_bit            BIT NOT NULL,
test_tinyint        TINYINT NULL,
test_smallint       SMALLINT NULL,
test_int            INT NULL,
test_bigint         BIGINT NULL,
test_decimal        DECIMAL(18, 4) NULL,  -- Triggers PREC_SCALE logic
test_numeric        NUMERIC(10, 2) NULL,
test_smallmoney     SMALLMONEY NULL,
test_money          MONEY NULL,

-- Approximate Numerics
test_real           REAL NULL,            -- Maps to FLT4
test_float          FLOAT NULL,           -- Maps to FLT8

-- Date and Time
test_date           DATE NULL,
test_time           TIME(7) NULL,
test_datetime       DATETIME NULL,        -- Classic 8-byte datetime
test_datetime2      DATETIME2(7) NULL,    -- High precision
test_smalldatetime  SMALLDATETIME NULL,
test_dtoffset       DATETIMEOFFSET(7) NULL,

-- Character Strings (Non-Unicode)
test_char           CHAR(10) NULL,
test_varchar        VARCHAR(50) NULL,
test_varchar_max    VARCHAR(MAX) NULL,    -- Triggers PLP (Partially Length Prefixed)
test_text           TEXT NULL,            -- Legacy LOB

-- Unicode Strings
test_nchar          NCHAR(10) NULL,
test_nvarchar       NVARCHAR(50) NULL,
test_nvarchar_max   NVARCHAR(MAX) NULL,   -- Triggers PLP

-- Binary Strings
test_binary         BINARY(8) NULL,
test_varbinary      VARBINARY(50) NULL,
test_varbinary_max  VARBINARY(MAX) NULL,  -- Triggers PLP
test_image          IMAGE NULL,           -- Legacy LOB

-- Other
test_guid           UNIQUEIDENTIFIER NULL,
test_xml            XML NULL
);
""";

  private static final String insertSql = """
-- Insert Test Data
INSERT INTO dbo.AllDataTypes (
test_bit, test_tinyint, test_smallint, test_int, test_bigint,
test_decimal, test_numeric, test_smallmoney, test_money,
test_real, test_float,
test_date, test_time, test_datetime, test_datetime2, test_smalldatetime, test_dtoffset,
test_char, test_varchar, test_varchar_max, test_text,
test_nchar, test_nvarchar, test_nvarchar_max,
test_binary, test_varbinary, test_varbinary_max, test_image,
test_guid, test_xml
) VALUES (
1,                                      -- BIT
255,                                    -- TINYINT
32000,                                  -- SMALLINT
2000000000,                             -- INT
9000000000000000000,                    -- BIGINT
12345.6789,                             -- DECIMAL
999.99,                                 -- NUMERIC
214.99,                                 -- SMALLMONEY
922337203685477.58,                     -- MONEY
123.45,                                 -- REAL
123456789.987654321,                    -- FLOAT
'2023-12-25',                           -- DATE
'14:30:15.1234567',                     -- TIME
'2023-12-25 14:30:00',                  -- DATETIME
'2023-12-25 14:30:15.1234567',          -- DATETIME2
'2023-12-25 14:30:00',                  -- SMALLDATETIME
'2023-12-25 14:30:15.1234567 +05:30',   -- DATETIMEOFFSET
'FixedChar',                            -- CHAR
'Variable Length String',               -- VARCHAR
REPLICATE('A', 5000),                   -- VARCHAR(MAX) (Large PLP)
'Legacy Text Data',                     -- TEXT
N'FixedUni',                            -- NCHAR
N'Unicode String',                      -- NVARCHAR
REPLICATE(N'あ', 4000),                  -- NVARCHAR(MAX) (Large PLP)
0xDEADBEEF,                             -- BINARY
0xCAFEBABE,                             -- VARBINARY
0xFEEDBACC,                             -- VARBINARY(MAX)
0x00112233,                             -- IMAGE
NEWID(),                                -- GUID
'<root><node>Test XML</node></root>'    -- XML
);
""";

  private static final String bindSql = """
INSERT INTO dbo.AllDataTypes (
  test_bit, test_tinyint, test_smallint, test_int, test_bigint,
  test_decimal, test_numeric, test_smallmoney, test_money,
  test_real, test_float,
  test_date, test_time, test_datetime, test_datetime2, test_smalldatetime, test_dtoffset,
  test_char, test_varchar, test_varchar_max, test_text,
  test_nchar, test_nvarchar, test_nvarchar_max,
  test_binary, test_varbinary, test_varbinary_max, test_image,
  test_guid, test_xml
) VALUES (
  @p0, @p1, @p2, @p3, @p4,
  @p5, @p6, @p7, @p8,
  @p9, @p10,
  @p11, @p12, @p13, @p14, @p15, @p16,
  @p17, @p18, @p19, @p20,
  @p21, @p22, @p23,
  @p24, @p25, @p26, @p27,
  @p28, @p29
)
""";
  private static final String querySql = """
SET TEXTSIZE -1; -- Disable the 4096 byte limit
SELECT * FROM dbo.AllDataTypes;
""";



}

