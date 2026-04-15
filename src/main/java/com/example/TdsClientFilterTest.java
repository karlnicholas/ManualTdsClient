package com.example;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.api.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsClientFilterTest {
  public static void main(String[] args) {
    new TdsClientFilterTest().run();
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

    System.out.println("Connecting to database for Transaction Testing...");

    // usingWhen ensures the connection is safely closed regardless of success or error
    Mono.usingWhen(
            Mono.from(connectionFactory.create()),
            this::runSql,
            conn -> Mono.from(conn.close()).doOnSuccess(v -> System.out.println("\nConnection safely closed."))
        )
        .doOnError(t -> System.err.println("Connection/Run Failed: " + t.getMessage()))
        .block();
  }

  public Mono<Void> runSql(Connection connection) {

    // 1. Protocol Noise Reduction (Type Filtering)
    // SQL Server sends string messages via PRINT, followed by tabular data.
    String sql1 = """
        PRINT 'Beginning noisy data extraction...';
        PRINT 'Loading variables...';
        SELECT id FROM dbo.AllDataTypes WHERE id = 1;
        """;
    Predicate<Result.Segment> filter1 = segment -> segment instanceof Result.RowSegment;
    Predicate<List<Integer>> validate1 = list -> list.size() == 1 && list.get(0) == 1;

    // 2. Multi-Result Set Demultiplexing
    // Executes two queries returning different table structures in one stream.
    String sql2 = """
        SELECT id, test_varchar FROM dbo.AllDataTypes WHERE id = 1;
        SELECT id, test_nvarchar FROM dbo.AllDataTypes WHERE id = 2;
        """;
    Predicate<Result.Segment> filter2 = segment -> {
      if (segment instanceof Result.RowSegment rowSeg) {
        // Only keep rows that belong to the second result set (contains test_nvarchar)
        return rowSeg.row().getMetadata().getColumnMetadatas().stream()
            .anyMatch(meta -> meta.getName().equalsIgnoreCase("test_nvarchar"));
      }
      return false;
    };
    Predicate<List<Integer>> validate2 = list -> list.size() == 1 && list.get(0) == 2;

// 3. Pre-Materialization Memory Shields
    // We read a primitive from the raw buffer and drop the row to avoid mapping unused objects.
    String sql3 = "SELECT id, test_varchar FROM dbo.AllDataTypes;";
    Predicate<Result.Segment> filter3 = segment -> {
      if (segment instanceof Result.RowSegment rowSeg) {
        String val = rowSeg.row().get("test_varchar", String.class);
        // FIX: Match the actual data inserted into test_varchar
        return "Euro: € and Cafe: Café".equals(val);
      }
      return false;
    };
    Predicate<List<Integer>> validate3 = list -> !list.isEmpty();

    // 4. Volatile / In-Memory State Filtering
    // Filtering based on a fast-changing local JVM cache instead of SQL WHERE clause.
    String sql4 = "SELECT id FROM dbo.AllDataTypes;";
    Set<Integer> locallyCachedIds = ConcurrentHashMap.newKeySet();
    locallyCachedIds.addAll(Set.of(1, 2)); // Simulate IDs we already processed

    Predicate<Result.Segment> filter4 = segment -> {
      if (segment instanceof Result.RowSegment rowSeg) {
        Integer id = rowSeg.row().get("id", Integer.class);
        return !locallyCachedIds.contains(id); // Drop if it's in our local cache
      }
      return false;
    };
    Predicate<List<Integer>> validate4 = list -> list.stream().noneMatch(locallyCachedIds::contains);

    // Standard mapper used across tests
    Function<Result, Publisher<Integer>> extractIdMapper = res ->
        res.map((row, meta) -> row.get("id", Integer.class));

    // Execution Chain
    return Mono.defer(() -> executeFilterTest("1. Protocol Noise Reduction", connection.createStatement(sql1).execute(), filter1, extractIdMapper, validate1))
        .then(Mono.defer(() -> executeFilterTest("2. Multi-Result Set Demultiplexing", connection.createStatement(sql2).execute(), filter2, extractIdMapper, validate2)))
        .then(Mono.defer(() -> executeFilterTest("3. Pre-Materialization Memory Shields", connection.createStatement(sql3).execute(), filter3, extractIdMapper, validate3)))
        .then(Mono.defer(() -> executeFilterTest("4. Volatile/In-Memory State Filtering", connection.createStatement(sql4).execute(), filter4, extractIdMapper, validate4)));
  }

  /**
   * Universal Async Helper that applies a Result.filter(), maps the data, and validates the output.
   */
  private <T> Mono<Void> executeFilterTest(
      String stepName,
      Publisher<? extends Result> resultPublisher,
      Predicate<Result.Segment> filter,
      Function<Result, Publisher<T>> mapper,
      Predicate<List<T>> validator) {

    System.out.println("\n--- Executing: " + stepName + " ---");

    return Flux.from(resultPublisher)
        // Apply the R2DBC Result.filter() native to the stream
        .map(result -> result.filter(filter))
        // Map the surviving rows
        .flatMap(mapper)
        // Collect to evaluate the final output
        .collectList()
        .doOnNext(list -> {
          if (validator.test(list)) {
            System.out.println("  -> [PASS] Condition met. Mapped items: " + list);
          } else {
            System.out.println("  -> [FAIL] Validation failed! Mapped items: " + list);
          }
        })
        .doOnError(error -> System.err.println("  -> [FAIL] Stream Error: " + error.getMessage()))
        .then()
        .onErrorResume(e -> Mono.empty());
  }
}