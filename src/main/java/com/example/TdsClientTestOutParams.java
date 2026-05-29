package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.OutParameters;
import io.r2dbc.spi.OutParametersMetadata;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class TdsClientTestOutParams {
  public static void main(String[] args) throws Exception {
    new TdsClientTestOutParams().run();
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

  @SuppressWarnings("JpaQueryApiInspection")
  public Mono<Void> runSql(Connection connection) {
    String outSql = "SELECT @count = COUNT(*), @sum = SUM(postCount), @average = AVG(postCount) FROM dbo.users";

    // 2. Return the Mono<Void> representing the entire operation
    return executeStreamNoLatch("5. Read Clob Length", connection.createStatement(outSql)
            .bind("@count", Parameters.out(R2dbcType.BIGINT))
            .bind("@sum", Parameters.out(R2dbcType.BIGINT))
            .bind("@average", Parameters.out(R2dbcType.BIGINT)).execute(),
        res -> res.flatMap(segment -> {
              if (segment instanceof Result.OutSegment outSeg) {
                OutParameters out = outSeg.outParameters();
                return Flux.just(rvOutMapper.apply(out, out.getMetadata()));
              }
              return Flux.empty();
            }));
  }

  // --- The Universal Async Helper (LATCH-FREE) ---

  private <T> Mono<Void> executeStreamNoLatch(String stepName, Publisher<? extends Result> resultPublisher, Function<Result, Publisher<T>> extractor) {
    System.out.println("\n--- Executing: " + stepName + " ---");

    // 4. Build the reactive pipeline without subscribing or blocking
    return Flux.from(resultPublisher)
        .flatMap(extractor)
        .doOnNext(item -> System.out.println("  -> " + item))
        .doOnError(error -> System.err.println("[" + stepName + "] Stream Error: " + error.getMessage()))
        .doOnComplete(() -> System.out.println("--- Completed: " + stepName + " ---"))
        .then(); // .then() converts Flux<T> into a Mono<Void> that completes when the Flux finishes
  }

  BiFunction<OutParameters, OutParametersMetadata, List<Integer>> rvOutMapper = (out, meta) -> List.of(
      out.get(0, Integer.class),
      out.get(1, Integer.class),
      out.get(2, Integer.class)
  );

}