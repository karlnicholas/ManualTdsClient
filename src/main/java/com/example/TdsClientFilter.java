package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Result.OutSegment;
import io.r2dbc.spi.Result.RowSegment;
import io.r2dbc.spi.Result.Segment;
import io.r2dbc.spi.Result.UpdateCount;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
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

public class TdsClientFilter {
  public static void main(String[] args) {
    new TdsClientFilter().run();
  }

  private void run() {
    String r2dbcUrl = "r2dbc:mssql://reactnonreact:reactnonreact@localhost:1433/reactnonreact?trustServerCertificate=true";
    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(r2dbcUrl)).initialSize(2).maxSize(50)
.build());

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

  public Mono<Void> runSql(Connection connection) {

    // --- STANDARD FILTER TESTS ---

    // 1. Protocol Noise Reduction (Type Filtering)
    String sql1 = """
        PRINT 'Beginning noisy data extraction...';
        PRINT 'Loading variables...';
        SELECT id FROM dbo.AllDataTypes WHERE id = 1;
        """;
    Predicate<Segment> filter1 = segment -> segment instanceof RowSegment;
    Predicate<List<Integer>> validate1 = list -> list.size() == 1 && list.get(0) == 1;

    // 2. Multi-Result Set Demultiplexing
    String sql2 = """
        SELECT id, test_varchar FROM dbo.AllDataTypes WHERE id = 1;
        SELECT id, test_nvarchar FROM dbo.AllDataTypes WHERE id = 2;
        """;
    Predicate<Segment> filter2 = segment -> {
      if (segment instanceof RowSegment rowSeg) {
        return rowSeg.row().getMetadata().getColumnMetadatas().stream()
            .anyMatch(meta -> meta.getName().equalsIgnoreCase("test_nvarchar"));
      }
      return false;
    };
    Predicate<List<Integer>> validate2 = list -> list.size() == 1 && list.get(0) == 2;

    // 3. Pre-Materialization Memory Shields
    String sql3 = "SELECT id, test_varchar FROM dbo.AllDataTypes;";
    Predicate<Segment> filter3 = segment -> {
      if (segment instanceof RowSegment rowSeg) {
        String val = rowSeg.row().get("test_varchar", String.class);
        return "Euro: € and Cafe: Café".equals(val);
      }
      return false;
    };
    Predicate<List<Integer>> validate3 = list -> !list.isEmpty();

    // 4. Volatile / In-Memory State Filtering
    String sql4 = "SELECT id FROM dbo.AllDataTypes;";
    Set<Integer> locallyCachedIds = ConcurrentHashMap.newKeySet();
    locallyCachedIds.addAll(Set.of(1, 2));

    Predicate<Segment> filter4 = segment -> {
      if (segment instanceof RowSegment rowSeg) {
        Integer id = rowSeg.row().get("id", Integer.class);
        return !locallyCachedIds.contains(id);
      }
      return false;
    };
    Predicate<List<Integer>> validate4 = list -> list.stream().noneMatch(locallyCachedIds::contains);

    // 5. UpdateCount Segment Filtering
    String sql5 = "UPDATE dbo.AllDataTypes SET test_varchar = test_varchar WHERE id = 1;";
    Predicate<Segment> filter5 = seg -> seg instanceof UpdateCount;
    Function<Result, Publisher<Long>> updateCountMapper = res -> res.flatMap(seg -> {
      if (seg instanceof UpdateCount ucs) {
        return Mono.just(ucs.value());
      }
      return Mono.empty();
    });
    Predicate<List<Long>> validate5 = list -> !list.isEmpty();

    // 6. OutParameters Segment Filtering
    Publisher<? extends Result> pub6 = connection.createStatement("EXEC sp_executesql @stmt, @params, @outVal OUTPUT")
        .bind("@stmt", "SET @outVal = 999")
        .bind("@params", "@outVal INT OUTPUT")
        .bind("@outVal", Parameters.out(R2dbcType.INTEGER))
        .execute();

    Predicate<Segment> filter6 = seg -> seg instanceof OutSegment;
    Function<Result, Publisher<Integer>> outParamMapper = res -> res.flatMap(seg -> {
      if (seg instanceof OutSegment outSeg) {
        return Mono.just(outSeg.outParameters().get(0, Integer.class));
      }
      return Mono.empty();
    });
    Predicate<List<Integer>> validate6 = list -> list.contains(999);


    // --- ERROR & RESILIENCE TESTS ---

    // 7. Mid-Stream Cancellation (Reactive Backpressure Cancel via take)
    String sql7 = "SELECT object_id AS id FROM sys.all_objects";
    Predicate<Segment> filter7 = seg -> seg instanceof RowSegment;
    Function<Flux<Integer>, Flux<Integer>> cancelOperator = flux -> flux.take(5);
    Predicate<List<Integer>> validate7 = list -> list.size() == 5;

    // 8. Exception Thrown Inside Filter Predicate
    String sql8 = "SELECT 1 AS id UNION ALL SELECT 2 AS id";
    Predicate<Segment> filter8 = seg -> {
      if (seg instanceof RowSegment) {
        throw new RuntimeException("Intentional RuntimeException thrown inside Java filter!");
      }
      return true;
    };

    // 9. Database Error Mid-Stream
    String sql9 = "SELECT 1 AS id; RAISERROR('Intentional Native Database Error Mid-Stream', 16, 1); SELECT 2 AS id;";
    Predicate<Segment> filter9 = seg -> seg instanceof RowSegment;

    // 10. Post-Trauma Sanity Check
    String sql10 = "SELECT 42 AS id;";
    Predicate<Segment> filter10 = seg -> seg instanceof RowSegment;
    Predicate<List<Integer>> validate10 = list -> list.size() == 1 && list.get(0) == 42;


    // Standard row mapper used across normal tests
    Function<Result, Publisher<Integer>> extractIdMapper = res -> res.flatMap(seg -> {
      if (seg instanceof RowSegment rs) {
        return Mono.just(rs.row().get("id", Integer.class));
      }
      return Mono.empty();
    });

    // Execution Chain
    return Mono.defer(() -> executeFilterTest("1. Protocol Noise Reduction", connection.createStatement(sql1).execute(), filter1, extractIdMapper, null, validate1, false))
        .then(Mono.defer(() -> executeFilterTest("2. Multi-Result Set Demultiplexing", connection.createStatement(sql2).execute(), filter2, extractIdMapper, null, validate2, false)))
        .then(Mono.defer(() -> executeFilterTest("3. Pre-Materialization Memory Shields", connection.createStatement(sql3).execute(), filter3, extractIdMapper, null, validate3, false)))
        .then(Mono.defer(() -> executeFilterTest("4. Volatile/In-Memory State Filtering", connection.createStatement(sql4).execute(), filter4, extractIdMapper, null, validate4, false)))
        .then(Mono.defer(() -> executeFilterTest("5. UpdateCount Segment Filtering", connection.createStatement(sql5).execute(), filter5, updateCountMapper, null, validate5, false)))
        .then(Mono.defer(() -> executeFilterTest("6. Out Parameters Segment Filtering", pub6, filter6, outParamMapper, null, validate6, false)))
        .then(Mono.defer(() -> executeFilterTest("7. Mid-Stream Cancellation (.take(5))", connection.createStatement(sql7).execute(), filter7, extractIdMapper, cancelOperator, validate7, false)))
        .then(Mono.defer(() -> executeFilterTest("8. Exception inside Filter Predicate", connection.createStatement(sql8).execute(), filter8, extractIdMapper, null, null, true)))
        .then(Mono.defer(() -> executeFilterTest("9. Database Error Mid-Stream", connection.createStatement(sql9).execute(), filter9, extractIdMapper, null, null, true)))
        .then(Mono.defer(() -> executeFilterTest("10. Post-Trauma Driver Sanity Check", connection.createStatement(sql10).execute(), filter10, extractIdMapper, null, validate10, false)));
  }

  /**
   * Universal Async Helper that applies a Result.filter(), maps the data, applies reactive operators, and validates the output.
   */
  private <T> Mono<Void> executeFilterTest(
      String stepName,
      Publisher<? extends Result> resultPublisher,
      Predicate<Segment> filter,
      Function<Result, Publisher<T>> mapper,
      Function<Flux<T>, Flux<T>> reactiveOperator,
      Predicate<List<T>> validator,
      boolean expectError) {

    System.out.println("\n--- Executing: " + stepName + " ---");

    Flux<T> baseStream = Flux.from(resultPublisher)
        // Apply the R2DBC Result.filter() native to the stream
        .map(result -> result.filter(filter))
        // Map the surviving rows/segments
        .flatMap(mapper);

    // Apply specific reactive operators (like .take() for cancellation) if provided
    Flux<T> executionStream = reactiveOperator != null ? reactiveOperator.apply(baseStream) : baseStream;

    return executionStream
        // Collect to evaluate the final output
        .collectList()
        .doOnNext(list -> {
          if (expectError) {
            System.out.println("  -> [FAIL] Expected an error, but stream completed normally! Mapped items: " + list);
            throw new IllegalStateException("Expected an error but got a success signal.");
          } else if (validator != null && validator.test(list)) {
            System.out.println("  -> [PASS] Condition met. Mapped items: " + list);
          } else {
            System.out.println("  -> [FAIL] Validation failed! Mapped items: " + list);
          }
        })
        .then()
        .onErrorResume(e -> {
          if (e instanceof IllegalStateException) {
            return Mono.error(e); // Let execution flow halt if logic was broken
          }
          if (expectError) {
            System.out.println("  ✓ [PASS] Graceful Rejection Caught: " + e.getClass().getSimpleName() + " - " + e.getMessage());
            return Mono.empty();
          } else {
            System.err.println("  -> [FAIL] Unexpected Stream Error: " + e.getMessage());
            return Mono.empty();
          }
        });
  }
}