package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Statement;
import org.tdslib.r2dbc.mssql.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsClientBindMassiveRpc {

  public static void main(String[] args) {
    new TdsClientBindMassiveRpc().run();
  }

  private void run() {
    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(DRIVER, "javatdslib").option(HOST, "localhost").option(PORT, 1433)
        .option(USER, "reactnonreact").option(PASSWORD, "reactnonreact").option(DATABASE, "reactnonreact")
        .option(TdsLibOptions.TRUST_SERVER_CERTIFICATE, true).build())).initialSize(10).maxSize(10).build());

    System.out.println("Connecting to database pool for Massive RPC Framework Testing...");

    Mono.usingWhen(Mono.just(pool), this::runSuite, ConnectionPool::disposeLater)
        .doOnError(t -> System.err.println("\n❌ Test Suite Failed: " + t.getMessage()))
        .block();
  }

  public Mono<Void> runSuite(ConnectionPool pool) {
    return Mono.defer(() -> testMassiveSqlStatement(pool))
        .then(Mono.defer(() -> testMassiveParameterDeclaration(pool)));
  }

  /**
   * Test 1: Forces the @stmt RPC parameter to exceed 8,000 bytes.
   * 10,000 characters * 2 bytes (UTF-16LE) = 20,000 byte SQL string.
   */
  private Mono<Void> testMassiveSqlStatement(ConnectionPool pool) {
    System.out.println("\n--- Test 1: Massive SQL Statement (@stmt > 8KB) ---");

    // Create a 10,000 character comment to pad the SQL statement
    String massivePadding = "/* " + "A".repeat(10000) + " */";
    String sql = "SELECT 1 AS result " + massivePadding;

    return Mono.usingWhen(
        Mono.from(pool.create()),
        conn -> Flux.from(conn.createStatement(sql).execute())
            .flatMap(res -> res.map((row, meta) -> row.get(0, Integer.class)))
            .doOnNext(val -> {
              System.out.println("  -> Server executed query and returned: " + val);
              if (val != 1) {
                throw new IllegalStateException("Expected result '1', got: " + val);
              }
              System.out.println("  ✓ [OK] Successfully streamed massive @stmt as NVARCHAR(MAX) PLP");
            })
            .then(),
        Connection::close
    );
  }

  /**
   * Test 2: Forces the @params RPC parameter to exceed 8,000 bytes.
   * Binding 600 parameters generates a massive declaration string:
   * "@p0 int, @p1 int, @p2 int..." which easily exceeds the 8KB limit.
   */
  private Mono<Void> testMassiveParameterDeclaration(ConnectionPool pool) {
    System.out.println("\n--- Test 2: Massive Parameter List (@params > 8KB) ---");

    int parameterCount = 600;
    StringBuilder sqlBuilder = new StringBuilder("SELECT ");

    // Dynamically build: SELECT @p0 + @p1 + @p2 ... AS total
    for (int i = 0; i < parameterCount; i++) {
      if (i > 0) {
        sqlBuilder.append(" + ");
      }
      sqlBuilder.append("@p").append(i);
    }
    sqlBuilder.append(" AS total");

    return Mono.usingWhen(
        Mono.from(pool.create()),
        conn -> {
          Statement stmt = conn.createStatement(sqlBuilder.toString());

          // Bind all 600 parameters to the value '1'
          for (int i = 0; i < parameterCount; i++) {
            stmt.bind("p" + i, 1);
          }

          return Flux.from(stmt.execute())
              .flatMap(res -> res.map((row, meta) -> row.get(0, Integer.class)))
              .doOnNext(val -> {
                System.out.println("  -> Server summed 600 parameters. Result: " + val);
                if (val != parameterCount) {
                  throw new IllegalStateException("Expected sum '" + parameterCount + "', got: " + val);
                }
                System.out.println("  ✓ [OK] Successfully streamed massive @params declaration as NVARCHAR(MAX) PLP");
              })
              .then();
        },
        Connection::close
    );
  }
}