package com.example;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.api.TdsConnectionFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsClientTestSelect {
  public static void main(String[] args) throws Exception {
    new TdsClientTestSelect().run();
  }

  private void run() throws Exception {
    ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(ConnectionFactoryOptions.DRIVER, "javatdslib")
        .option(HOST, "localhost")
        .option(PORT, 1433)
        .option(PASSWORD, "reactnonreact")
        .option(USER, "reactnonreact")
        .option(DATABASE, "reactnonreact")
        .option(TdsConnectionFactory.TRUST_SERVER_CERTIFICATE, true)
        .build());

    System.out.println("Connecting to database...");

    // Use Mono to handle the single connection event safely
    Mono.from(connectionFactory.create())
        .doOnNext(conn -> {
          try {
            runSql(conn);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        })
        .doOnError(t -> System.err.println("Connection/Run Failed: " + t.getMessage()))
        // block() natively waits for the entire pipeline to finish, no latch needed here!
        .block();
  }

  @SuppressWarnings("JpaQueryApiInspection")
  private void runSql(Connection connection) throws InterruptedException {
    // 5. Select All (DQL -> Mapping)
    executeStream("5. Select All", connection.createStatement(querySql).execute(), res -> res.map(namesDataTypesMapper));

    // Example of how a user would gracefully close it:
    Mono.from(connection.close())
        .doFinally(signal -> System.out.println("Connection safely closed."))
        .subscribe();
  }

  // --- The Universal Async Helper using Reactor Flux ---

  private <T> void executeStream(String stepName, Publisher<? extends Result> resultPublisher, Function<Result, Publisher<T>> extractor) throws InterruptedException {
    System.out.println("\n--- Executing: " + stepName + " ---");
    CountDownLatch latch = new CountDownLatch(1);

    Flux.from(resultPublisher)
        .flatMap(extractor)
        .subscribe(
            item -> System.out.println("  -> " + item),
            error -> {
              System.err.println("[" + stepName + "] Stream Error: " + error.getMessage());
              latch.countDown();
            },
            () -> {
              System.out.println("--- Completed: " + stepName + " ---");
              latch.countDown();
            }
        );

    latch.await();
  }

  // --- Mappers ---

//  BiFunction<Row, RowMetadata, AllDataTypesRecord> allDataTypesMapper = (row, meta) -> new AllDataTypesRecord(
//      row.get(0, Integer.class),          row.get(1, Boolean.class),
//      row.get(2, Byte.class),             row.get(3, Short.class),
//      row.get(4, Integer.class),          row.get(5, Long.class),
//      row.get(6, BigDecimal.class),       row.get(7, BigDecimal.class),
//      row.get(8, BigDecimal.class),       row.get(9, BigDecimal.class),
//      row.get(10, Float.class),           row.get(11, Double.class),
//      row.get(12, LocalDate.class),       row.get(13, LocalTime.class),
//      row.get(14, LocalDateTime.class),   row.get(15, LocalDateTime.class),
//      row.get(16, LocalDateTime.class),   row.get(17, OffsetDateTime.class),
//      row.get(18, String.class),
//      row.get(19, String.class),
//      row.get(20, String.class),          row.get(21, String.class),
//      row.get(22, String.class),          row.get(23, String.class),
//      row.get(24, String.class),          row.get(25, byte[].class),
//      row.get(26, byte[].class),          row.get(27, byte[].class),
//      row.get(28, byte[].class),          row.get(29, UUID.class),
//      row.get(30, String.class)
//  );

//  test_bit, test_tinyint, test_smallint, test_int, test_bigint, test_decimal, test_numeric,
//  test_smallmoney, test_money, test_real, test_float, test_date, test_time, test_datetime,
//  test_datetime2, test_smalldatetime, test_dtoffset, test_char, test_varchar, test_varchar_max,
//  test_text, test_nchar, test_nvarchar, test_nvarchar_max, test_binary, test_varbinary,
//  test_varbinary_max, test_image, test_guid, test_xml

//  BiFunction<Row, RowMetadata, AllDataTypesRecord> namesDataTypesMapper = (row, meta) -> new AllDataTypesRecord(
//      row.get("id", Integer.class),
//      row.get("test_bit", Boolean.class),
//      row.get("test_tinyint", Byte.class),
//      row.get("test_smallint", Short.class),
//      row.get("test_int", Integer.class),
//      row.get("test_bigint", Long.class),
//      row.get("test_decimal", BigDecimal.class),
//      row.get("test_numeric", BigDecimal.class),
//      row.get("test_smallmoney", BigDecimal.class),
//      row.get("test_money", BigDecimal.class),
//      row.get("test_real", Float.class),
//      row.get("test_float", Double.class),
//      row.get("test_date", LocalDate.class),
//      row.get("test_time", LocalTime.class),
//      row.get("test_datetime", LocalDateTime.class),
//      row.get("test_datetime2", LocalDateTime.class),
//      row.get("test_smalldatetime", LocalDateTime.class),
//      row.get("test_dtoffset", OffsetDateTime.class),
//      row.get("test_char", String.class),
//      row.get("test_varchar", String.class),
//      row.get("test_varchar_max", String.class),
//      row.get("test_text", String.class),
//      row.get("test_nchar", String.class),
//      row.get("test_nvarchar", String.class),
//      row.get("test_nvarchar_max", String.class),
//      row.get("test_binary", byte[].class),
//      row.get("test_varbinary", byte[].class),
//      row.get("test_varbinary_max", byte[].class),
//      row.get("test_image", byte[].class),
//      row.get("test_guid", UUID.class),
//      row.get("test_xml", String.class)
//  );
//
  BiFunction<Row, RowMetadata, List> namesDataTypesMapper = (row, meta) -> List.of(
//    row.get("test_tinyint", Object.class),
//    row.get("test_int", Object.class),
//    row.get("test_text", Object.class),
//    row.get("test_time", Object.class),
//    row.get("test_smalldatetime", Object.class),
//    row.get("test_bit", Object.class),
//    row.get("test_real", Object.class),
//    row.get("test_nchar", Object.class),
//    row.get("test_varbinary", Object.class),
    row.get("test_varbinary_max", Object.class),
    row.get("test_dtoffset", Object.class)
  );
//  BiFunction<Row, RowMetadata, List> namesDataTypesMapper = (row, meta) -> List.of(
//      row.get("test_tinyint", Object.class),
//      row.get("test_int", Object.class),
//      row.get("test_text", Object.class),
//      row.get("test_time", Object.class),
//      row.get("test_smalldatetime", Object.class),
//      row.get("test_bit", Object.class),
//      row.get("test_real", Object.class),
//      row.get("test_nchar", Object.class),
//      row.get("test_varbinary", Object.class),
//      row.get("test_varbinary_max", Object.class),
//      row.get("test_dtoffset", Object.class)
//  );
  private static final String querySql = """
SET TEXTSIZE -1;
SELECT test_varbinary_max, test_dtoffset 
FROM dbo.AllDataTypes
WHERE id=1;
""";
//  private static final String querySql = """
//SET TEXTSIZE -1;
//SELECT test_tinyint, test_int, test_text, test_time, test_smalldatetime, test_bit, test_real, test_nchar, test_varbinary, test_varbinary_max, test_dtoffset
//FROM dbo.AllDataTypes
//WHERE id BETWEEN 1 AND 3;
//""";
//  private static final String querySql = """
//SET TEXTSIZE -1;
//SELECT *
//FROM dbo.AllDataTypes
//WHERE id BETWEEN 1 AND 3;
//""";
}