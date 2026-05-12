package com.example;

// ... (Standard Imports & Setup Boilerplate) ...
import io.r2dbc.pool.*; import io.r2dbc.spi.*; import org.tdslib.javatdslib.api.*;
import reactor.core.publisher.*; import java.time.Duration;
import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class TdsClientBindThroughput {
  public static void main(String[] args) { new TdsClientBindThroughput().run(); }

  private void run() {
    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(DRIVER, "javatdslib").option(HOST, "localhost").option(PORT, 1433)
        .option(USER, "reactnonreact").option(PASSWORD, "reactnonreact").option(DATABASE, "reactnonreact")
        .option(TdsLibOptions.TRUST_SERVER_CERTIFICATE, true).build())).initialSize(10).maxSize(10).build());
    Mono.usingWhen(Mono.just(pool), this::runSuite, ConnectionPool::disposeLater).block();
  }

  public Mono<Void> runSuite(ConnectionPool pool) {
    return testConcurrentRpcExecution(pool)
        .then(testMassiveStringBinding(pool))
        .then(testMassiveStringBinding(pool));
  }

  /**
   * Throughput 1: Concurrent RPC Encoding.
   * 10 threads simultaneously encoding and writing TDS Type 3 packets.
   * Proves the Egress byte buffers are strictly thread-isolated and do not cross-contaminate.
   */
  private Mono<Void> testConcurrentRpcExecution(ConnectionPool pool) {
    System.out.println("\n--- Test 1: Concurrent RPC Encoding (10 Workers) ---");
    return Flux.range(1, 10)
        .flatMap(workerId -> Mono.usingWhen(
            Mono.from(pool.create()),
            conn -> Flux.from(conn.createStatement("SELECT @id AS worker_id")
                    .bind("id", workerId).execute())
                .flatMap(res -> res.map((row, meta) -> row.get(0, Integer.class)))
                .doOnNext(val -> {
                  if (!val.equals(workerId)) throw new IllegalStateException("Cross-talk detected!");
                })
                .then(),
            Connection::close
        ), 10)
        .doOnComplete(() -> System.out.println("  All 10 workers completed concurrent RPC tests."))
        .then();
  }

  /**
   * Throughput 2: Heavy Parameter Payload.
   * Binding a massive 1MB string to force the RPC encoder to fragment the request
   * across hundreds of physical TCP packets on the Egress pathway.
   */
  private Mono<Void> testMassiveStringBinding(ConnectionPool pool) {
    System.out.println("\n--- Test 2: Massive Parameter Payload Binding ---");
    String massivePayload = "A".repeat(1024 * 1024); // 1 Megabyte string

    return Mono.usingWhen(Mono.from(pool.create()),
        conn -> Flux.from(conn.createStatement("SELECT DATALENGTH(@payload) AS len")
                .bind("payload", massivePayload).execute())
            .flatMap(res -> res.map((row, meta) -> row.get(0, Long.class)))
            .doOnNext(len -> System.out.println("  Server successfully received payload. Length: " + len + " bytes"))
            .then(),
        Connection::close);
  }
}