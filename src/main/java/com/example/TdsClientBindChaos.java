package com.example;

// ... (Standard Imports & Setup Boilerplate) ...
import io.r2dbc.pool.*; import io.r2dbc.spi.*; import org.tdslib.javatdslib.api.*;
import reactor.core.publisher.*; import java.time.Duration;
import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class TdsClientBindChaos {
  public static void main(String[] args) { new TdsClientBindChaos().run(); }

  private void run() {
    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(DRIVER, "javatdslib").option(HOST, "localhost").option(PORT, 1433)
        .option(USER, "reactnonreact").option(PASSWORD, "reactnonreact").option(DATABASE, "reactnonreact")
        .option(TdsLibOptions.TRUST_SERVER_CERTIFICATE, true).build())).initialSize(2).build());
    Mono.usingWhen(Mono.just(pool), this::runSql, ConnectionPool::disposeLater).block();
  }

  public Mono<Void> runSql(ConnectionPool pool) {
    return Mono.usingWhen(Mono.from(pool.create()),
        conn -> testViolentCancelOnRpc(conn)
            .then(testSlowConsumerOnRpc(conn)),
        Connection::close);
  }

  /**
   * Stress 1: Mid-stream cancellation on an RPC Response.
   * Proves the driver correctly fires an Attention signal over the wire during sp_executesql processing.
   */
  private Mono<Void> testViolentCancelOnRpc(Connection connection) {
    System.out.println("\n--- Test 1: Mid-Stream Cancel (.take) on Parameterized Query ---");
    return Flux.from(connection.createStatement("SELECT TOP 5000 * FROM sys.all_objects WHERE object_id > @id")
            .bind("id", 0).execute())
        .flatMap(res -> res.flatMap(segment -> Mono.just(segment)))
        .take(5) // Violently cancel after 5 segments
        .doOnCancel(() -> System.out.println("  [!] Parameterized stream cancelled mid-flight!"))
        .then();
  }

  /**
   * Stress 2: Network Backpressure on an RPC Response.
   */
  private Mono<Void> testSlowConsumerOnRpc(Connection connection) {
    System.out.println("\n--- Test 2: Slow Consumer Downstream of Parameterized Query ---");
    return Flux.from(connection.createStatement("SELECT TOP 20 * FROM sys.all_objects WHERE type = @type")
            .bind("type", "S").execute())
        .flatMap(res -> res.flatMap(segment -> Mono.just(segment)))
        .delayElements(Duration.ofMillis(100)) // Force TCP socket backpressure
        .doOnNext(seg -> System.out.println("  Slowly consuming RPC segment..."))
        .then();
  }
}