package com.example;

import io.r2dbc.pool.*;
import io.r2dbc.spi.*;
import org.tdslib.r2dbc.mssql.*;
import reactor.core.publisher.*;
import java.time.Duration;
import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class TdsClientFilterChaos {
  public static void main(String[] args) { new TdsClientFilterChaos().run(); }

  private void run() {
    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(DRIVER, "javatdslib").option(HOST, "localhost").option(PORT, 1433)
        .option(USER, "reactnonreact").option(PASSWORD, "reactnonreact").option(DATABASE, "reactnonreact")
        .option(TdsLibOptions.TRUST_SERVER_CERTIFICATE, true).build())).initialSize(2).build());
    Mono.usingWhen(Mono.just(pool), this::runSql, ConnectionPool::disposeLater).block();
  }

  public Mono<Void> runSql(ConnectionPool pool) {
    return Mono.usingWhen(Mono.from(pool.create()),
        conn -> testFilterWithTake(conn)
            .then(testFilterWithSlowConsumer(conn)),
        Connection::close);
  }

  /**
   * Stress 1: Violent Cancellation on a Filtered Stream.
   * Proves the driver sends an Attention signal even if the items are being discarded by a filter.
   */
  private Mono<Void> testFilterWithTake(Connection connection) {
    System.out.println("\n--- Test 1: Mid-Stream Cancel (.take) on Filtered Result ---");
    return Flux.from(connection.createStatement("SELECT TOP 1000 * FROM sys.all_objects").execute())
        .map(result -> result.filter(segment -> segment instanceof Result.RowSegment))
        // In R2DBC 1.0, Result is not a Publisher. We extract segments via flatMap.
        .flatMap(res -> res.flatMap(segment -> Mono.just(segment)))
        .take(5) // Violently cancel after 5 segments
        .doOnCancel(() -> System.out.println("  [!] Filtered stream cancelled mid-flight!"))
        .then();
  }

  /**
   * Stress 2: Network Backpressure on a Filtered Stream.
   * The driver must buffer incoming tokens because the mapped stream is consuming slowly,
   * even though the filter itself executes instantly.
   */
  private Mono<Void> testFilterWithSlowConsumer(Connection connection) {
    System.out.println("\n--- Test 2: Slow Consumer Downstream of Filter ---");
    return Flux.from(connection.createStatement("SELECT TOP 20 * FROM sys.all_objects").execute())
        .map(result -> result.filter(segment -> segment instanceof Result.RowSegment))
        .flatMap(res -> res.flatMap(segment -> Mono.just(segment)))
        .delayElements(Duration.ofMillis(100)) // Force TCP socket backpressure
        .doOnNext(seg -> System.out.println("  Slowly consuming filtered segment..."))
        .then();
  }
}