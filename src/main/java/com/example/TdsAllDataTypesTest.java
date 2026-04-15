package com.example;

import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Row;
import org.tdslib.javatdslib.api.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.UUID;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsAllDataTypesTest {

  public static void main(String[] args) {
    new TdsAllDataTypesTest().run();
  }

  private void run() {
    ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(DRIVER, "javatdslib")
        .option(HOST, "localhost")
        .option(PORT, 1433)
        .option(USER, "reactnonreact")
        .option(PASSWORD, "reactnonreact")
        .option(DATABASE, "reactnonreact")
        .option(TdsLibOptions.TRUST_SERVER_CERTIFICATE, true)
        .build());

    Mono.from(connectionFactory.create())
        .flatMap(conn -> runSql(conn)
            .then(Mono.from(conn.close())))
        .doOnError(e -> System.err.println("Test Failed: " + e.getMessage()))
        .block();
  }

  public Mono<Void> runSql(Connection connection) {
    // Query rearranged: Standard types first, LOB (_max) types last
    String sql = "SET TEXTSIZE -1; SELECT " +
        "id, test_bit, test_tinyint, test_smallint, test_int, test_bigint, " +
        "test_decimal, test_numeric, test_smallmoney, test_money, test_real, test_float, " +
        "test_date, test_time, test_datetime, test_datetime2, test_smalldatetime, test_dtoffset, " +
        "test_char, test_varchar, test_text, test_nchar, test_nvarchar, test_binary, " +
        "test_varbinary, test_image, test_guid, test_xml, " +
//        "test_varchar_max " +
//        "test_varchar_max, test_nvarchar_max " +
        "test_varchar_max, test_varbinary_max " +
        "FROM dbo.AllDataTypes WHERE id = 1;";

//    String sql = "SET TEXTSIZE -1; SELECT " +
//        "test_varchar_max, test_varbinary_max " +
//        "FROM dbo.AllDataTypes WHERE id = 1;";


    System.out.println("Executing Comprehensive Data Type Test...");

    return Flux.from(connection.createStatement(sql).execute())
        .flatMap(result -> result.map((row, meta) -> {
          System.out.println("--- Mapping All Standard Columns ---");

          // Numeric Types
          printColumn(row, "id", Integer.class);
          printColumn(row, "test_bit", Boolean.class);
          printColumn(row, "test_tinyint", Byte.class);
          printColumn(row, "test_smallint", Short.class);
          printColumn(row, "test_int", Integer.class);
          printColumn(row, "test_bigint", Long.class);
          printColumn(row, "test_decimal", BigDecimal.class);
          printColumn(row, "test_numeric", BigDecimal.class);
          printColumn(row, "test_smallmoney", BigDecimal.class);
          printColumn(row, "test_money", BigDecimal.class);
          printColumn(row, "test_real", Float.class);
          printColumn(row, "test_float", Double.class);

          // Date/Time Types
          printColumn(row, "test_date", LocalDate.class);
          printColumn(row, "test_time", LocalTime.class);
          printColumn(row, "test_datetime", LocalDateTime.class);
          printColumn(row, "test_datetime2", LocalDateTime.class);
          printColumn(row, "test_smalldatetime", LocalDateTime.class);
          printColumn(row, "test_dtoffset", OffsetDateTime.class);

          // String/Text Types
          printColumn(row, "test_char", String.class);
          printColumn(row, "test_varchar", String.class);
          printColumn(row, "test_text", String.class);
          printColumn(row, "test_nchar", String.class);
          printColumn(row, "test_nvarchar", String.class);

          // Binary/Other Types (Standard length)
          printColumn(row, "test_binary", byte[].class);
          printColumn(row, "test_varbinary", byte[].class);
          printColumn(row, "test_image", byte[].class);
          printColumn(row, "test_guid", UUID.class);

          printColumn(row, "test_xml", String.class);

          // 2. Handle LOB Columns (Streaming)
//          streamClob(row, "test_varchar_max").doOnNext( length -> System.out.println("  -> test_varchar_max length: " + length));
//          return streamClob(row, "test_varchar_max").doOnNext( length -> System.out.println("  -> test_varchar_max length: " + length));
          return Flux.zip(
              streamClob(row, "test_varchar_max"),
              streamBlob(row, "test_varbinary_max")
          ).doOnNext(lengths -> {
            System.out.println("\n--- LOB Column Stream Results ---");
            System.out.println("  -> test_varchar_max length: " + lengths.getT1());
            System.out.println("  -> test_varbinary_max length: " + lengths.getT2());
          });
        }))
        .flatMap(f -> f)
        .then();
  }

  // Helper to print standard column values
  private void printColumn(Row row, String name, Class<?> type) {
    Object val = row.get(name, type);
    System.out.println("  -> " + name + ": " + val);
  }

  // Helper to stream Clob and return length
  private Mono<Long> streamClob(Row row, String name) {
    Clob clob = row.get(name, Clob.class);
    if (clob == null) return Mono.just(0L);
    return Flux.from(clob.stream())
        .reduce(0L, (acc, chunk) -> {
          System.out.println('.');
          return acc + chunk.length();
        });
  }

  // Helper to stream Blob and return length
  private Mono<Long> streamBlob(Row row, String name) {
    Blob blob = row.get(name, Blob.class);
    if (blob == null) return Mono.just(0L);
    return Flux.from(blob.stream())
        .reduce(0L, (acc, chunk) -> acc + chunk.remaining());
  }
}