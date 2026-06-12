package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ValidationDepth;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class TdsClientPlpVarious {
  public static void main(String[] args) {
    new TdsClientPlpVarious().run();
  }

  private void run() {
    String r2dbcUrl = "r2dbc:mssql://reactnonreact:reactnonreact@localhost:1433/reactnonreact?trustServerCertificate=true";

    // Constrained to exactly 1 connection to strictly test socket state.
    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(r2dbcUrl))
        .initialSize(1).maxSize(1).build());

    Mono.usingWhen(
            Mono.just(pool),
            this::runSql,
            p -> p.disposeLater().doOnSuccess(v -> System.out.println("\n✅ All permutations passed. Connection pool closed."))
        )
        .doOnError(t -> {
          System.err.println("\n❌ Test Suite Failed:");
          t.printStackTrace();
        })
        .block();
  }

  public Mono<Void> runSql(ConnectionPool pool) {
    return Mono.usingWhen(
        Mono.from(pool.create()),
        connection ->
            test_Alternating(connection)
                .then(test_AdjacentLobs(connection))
                .then(test_LobAtBeginning(connection))
                .then(test_LobAtEnd(connection))
            .then(test_EmptyAndNullLobs(connection))
                .then(checkConnection(connection))
        ,
        Connection::close
    );
  }

  // --- SCENARIO 1: Alternating Columns ---
  private Mono<Void> test_Alternating(Connection connection) {
    return Mono.defer(() -> {
      System.out.println("\n--- TEST 1: Alternating LOBs and Standard Columns ---");
      String sql = "SELECT 1 AS col1, REPLICATE(CAST('A' AS VARCHAR(MAX)), 150000) AS col2, 2 AS col3, REPLICATE(CAST('A' AS VARCHAR(MAX)), 150000) AS col4";

      return Flux.from(connection.createStatement(sql).execute())
          .flatMap(res -> res.map((row, metadata) -> row))
          .flatMap(row -> {
            Integer col1 = row.get(0, Integer.class);
            Clob clob1 = row.get(1, Clob.class);

            return Flux.from(clob1.stream())
                .reduce(0, (len, chunk) -> len + chunk.length())
                .flatMap(clob1Len -> {
                  // If we reach here safely, col3 has been buffered and the high-water mark advanced.
                  Integer col3 = row.get(2, Integer.class);
                  Clob clob2 = row.get(3, Clob.class);

                  return Flux.from(clob2.stream())
                      .reduce(0, (len, chunk) -> len + chunk.length())
                      .map(clob2Len -> col1 + col3 + clob1Len + clob2Len);
                });
          })
          .doOnNext(sum -> System.out.println("  [Success] Final Checksum: " + sum))
          .then();
    });
  }

  // --- SCENARIO 2: Adjacent LOBs ---
  private Mono<Void> test_AdjacentLobs(Connection connection) {
    return Mono.defer(() -> {
      System.out.println("\n--- TEST 2: Adjacent LOBs (LOB, LOB) ---");
      String sql = "SELECT REPLICATE(CAST('B' AS VARCHAR(MAX)), 200000) AS col1, REPLICATE(CAST('C' AS VARCHAR(MAX)), 250000) AS col2";

      return Flux.from(connection.createStatement(sql).execute())
          .flatMap(res -> res.map((row, metadata) -> row))
          .flatMap(row -> {
            Clob clob1 = row.get(0, Clob.class);

            return Flux.from(clob1.stream())
                .reduce(0, (len, chunk) -> len + chunk.length())
                .flatMap(clob1Len -> {
                  // Completion of clob1 triggered by the initialization of clob2
                  Clob clob2 = row.get(1, Clob.class);
                  return Flux.from(clob2.stream())
                      .reduce(0, (len, chunk) -> len + chunk.length())
                      .map(clob2Len -> clob1Len + clob2Len);
                });
          })
          .doOnNext(sum -> System.out.println("  [Success] Total LOB Bytes: " + sum))
          .then();
    });
  }

  // --- SCENARIO 3: LOB at the Beginning ---
  private Mono<Void> test_LobAtBeginning(Connection connection) {
    return Mono.defer(() -> {
      System.out.println("\n--- TEST 3: LOB First (LOB, Int, Int) ---");
      String sql = "SELECT REPLICATE(CAST('D' AS VARCHAR(MAX)), 100000) AS col1, 42 AS col2, 99 AS col3";

      return Flux.from(connection.createStatement(sql).execute())
          .flatMap(res -> res.map((row, metadata) -> row))
          .flatMap(row -> {
            Clob clob1 = row.get(0, Clob.class);

            return Flux.from(clob1.stream())
                .reduce(0, (len, chunk) -> len + chunk.length())
                .map(clob1Len -> {
                  // Completion deferred until col2 and col3 were fully parsed and placed in the array
                  Integer col2 = row.get(1, Integer.class);
                  Integer col3 = row.get(2, Integer.class);
                  return clob1Len + col2 + col3;
                });
          })
          .doOnNext(sum -> System.out.println("  [Success] Total calculated: " + sum))
          .then();
    });
  }

  // --- SCENARIO 4: LOB at the End ---
  private Mono<Void> test_LobAtEnd(Connection connection) {
    return Mono.defer(() -> {
      System.out.println("\n--- TEST 4: LOB Last (Int, Int, LOB) ---");
      String sql = "SELECT 10 AS col1, 20 AS col2, REPLICATE(CAST('E' AS VARCHAR(MAX)), 125000) AS col3";

      return Flux.from(connection.createStatement(sql).execute())
          .flatMap(res -> res.map((row, metadata) -> row))
          .flatMap(row -> {
            Integer col1 = row.get(0, Integer.class);
            Integer col2 = row.get(1, Integer.class);
            Clob clob1 = row.get(2, Clob.class);

            // Completion executed immediately upon PLP terminator (expectedColumns - 1)
            return Flux.from(clob1.stream())
                .reduce(0, (len, chunk) -> len + chunk.length())
                .map(clob1Len -> clob1Len + col1 + col2);
          })
          .doOnNext(sum -> System.out.println("  [Success] Total calculated: " + sum))
          .then();
    });
  }

  // --- SCENARIO 5: Empty and Null LOBs ---
  private Mono<Void> test_EmptyAndNullLobs(Connection connection) {
    return Mono.defer(() -> {
      System.out.println("\n--- TEST 5: Null & Empty Boundary Checks (Int, Null, Empty, Int) ---");
      String sql = "SELECT 7 AS col1, CAST(NULL AS VARCHAR(MAX)) AS col2, CAST('' AS VARCHAR(MAX)) AS col3, 11 AS col4";

      return Flux.from(connection.createStatement(sql).execute())
          .flatMap(res -> res.map((row, metadata) -> row))
          .map(row -> {
            // Evaluates the 0-byte PLP branch and standard null mapping
            Integer col1 = row.get(0, Integer.class);
            Clob nullClob = row.get(1, Clob.class); // Will map to null natively by R2DBC
            String emptyStr = row.get(2, String.class); // 0-byte PLP parsed as string
            Integer col4 = row.get(3, Integer.class);

            boolean isNullHandled = (nullClob == null);
            boolean isEmptyHandled = "".equals(emptyStr);

            if (!isNullHandled || !isEmptyHandled) {
              throw new IllegalStateException("Failed to parse empty/null LOB markers properly.");
            }

            return col1 + col4; // 7 + 11 = 18
          })
          .doOnNext(sum -> System.out.println("  [Success] Evaluated correctly. Sum: " + sum))
          .then();
    });
  }


  // --- SCENARIO 6: Network State Validation ---
  private Mono<Void> checkConnection(Connection connection) {
    return Mono.defer(() -> {
      System.out.println("\n--- TEST 6: Connection Validation ---");
      return Mono.from(connection.validate(ValidationDepth.REMOTE))
          .doOnNext(isValid -> {
            System.out.println("  Connection is valid: " + isValid);
            if (!isValid) {
              throw new IllegalStateException("Connection failed validation depth check. Desync occurred.");
            }
          })
          .then();
    });
  }
}