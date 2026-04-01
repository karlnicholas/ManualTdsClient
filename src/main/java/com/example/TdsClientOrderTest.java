package com.example;

import io.r2dbc.spi.Clob;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Row;
import org.tdslib.javatdslib.api.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsClientOrderTest {

  public static void main(String[] args) {
    new TdsClientOrderTest().run();
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

  private Mono<Void> runSql(Connection connection) {
    // Query rearranged: Standard types first, LOB (_max) types last
    String sql2 = "SET TEXTSIZE -1; SELECT " +
        "test_varchar_max, test_nvarchar_max " +
        "FROM dbo.AllDataTypes WHERE id = 1;";

    System.out.println("Executing Comprehensive Data Type Test...");

    return Flux.from(connection.createStatement(sql2).execute())
        .flatMap(result -> result.map((row, meta) -> {
          System.out.println("--- Mapping All Standard Columns ---");

          return streamClob(row, "test_nvarchar_max").doOnNext(length -> System.out.println("  -> test_nvarchar_max length: " + length))
              .then(streamClob(row, "test_varchar_max").doOnNext(length -> System.out.println("  -> test_varchar_max length: " + length)));
        }))
        .flatMap(f -> f)
        .then();
  }

  // Helper to stream Clob and return length
  private Mono<Long> streamClob(Row row, String name) {
    Clob clob = row.get(name, Clob.class);
    if (clob == null) return Mono.just(0L);
    return Flux.from(clob.stream())
        .reduce(0L, (acc, chunk) -> {
          System.out.println("Acc, chunk.length() = " + acc + " : " + chunk.length());
          return acc + chunk.length();
        });
  }

}