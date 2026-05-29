package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.CharBuffer;
import java.util.Arrays;

public class TdsClientBindThroughput {

  private static final int PAYLOAD_LENGTH = 1024 * 1024; // 1 Megabyte (1,048,576 chars)

  public static void main(String[] args) {
    new TdsClientBindThroughput().run();
  }

  private void run() {
    String r2dbcUrl = "r2dbc:mssql://reactnonreact:reactnonreact@localhost:1433/reactnonreact?trustServerCertificate=true";
    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(r2dbcUrl)).initialSize(2).maxSize(50)
.build());

    System.out.println("Connecting to database pool for LOB Egress Matrix Testing...");

    Mono.usingWhen(Mono.just(pool), this::runSuite, ConnectionPool::disposeLater)
        .doOnError(t -> System.err.println("\n❌ Test Suite Failed: " + t.getMessage()))
        .block();
  }

  public Mono<Void> runSuite(ConnectionPool pool) {
    return
        Mono.defer(()->testMassiveStringBinding(pool))
        .then(Mono.defer(()->testMassiveStringBinding(pool)))
        .then(Mono.defer(()->testMassiveVarCharBinding(pool)))
        .then(Mono.defer(()->testImplicitStringBinding(pool)))
        .then(Mono.defer(()->testExplicitStringBinding(pool)))
        .then(Mono.defer(()->testImplicitCharArrayBinding(pool)))
        .then(Mono.defer(()->testExplicitCharArrayBinding(pool)))
        .then(Mono.defer(()->testImplicitCharBufferBinding(pool)))
        .then(Mono.defer(()->testExplicitCharBufferBinding(pool)))
        ;
  }

  // --- STRING TESTS ---

  private Mono<Void> testImplicitStringBinding(ConnectionPool pool) {
    System.out.println("\n--- Test 1: Implicit String Binding ---");
    String payload = "A".repeat(PAYLOAD_LENGTH);
    return executeTest(pool, payload, PAYLOAD_LENGTH * 2L, "NVARCHAR(MAX) [2MB]");
  }

  private Mono<Void> testExplicitStringBinding(ConnectionPool pool) {
    System.out.println("\n--- Test 2: Explicit String Binding (VARCHAR) ---");
    String payload = "A".repeat(PAYLOAD_LENGTH);
    return executeTest(pool, Parameters.in(R2dbcType.VARCHAR, payload), (long) PAYLOAD_LENGTH, "VARCHAR(MAX) [1MB]");
  }

  // --- CHAR ARRAY TESTS ---

  private Mono<Void> testImplicitCharArrayBinding(ConnectionPool pool) {
    System.out.println("\n--- Test 3: Implicit char[] Binding ---");
    char[] payload = new char[PAYLOAD_LENGTH];
    Arrays.fill(payload, 'A');

    return executeTest(pool, payload, PAYLOAD_LENGTH * 2L, "NVARCHAR(MAX) [2MB]")
        .doFinally(signalType -> Arrays.fill(payload, '\0')); // Secure wipe
  }

  private Mono<Void> testExplicitCharArrayBinding(ConnectionPool pool) {
    System.out.println("\n--- Test 4: Explicit char[] Binding (VARCHAR) ---");
    char[] payload = new char[PAYLOAD_LENGTH];
    Arrays.fill(payload, 'A');

    return executeTest(pool, Parameters.in(R2dbcType.VARCHAR, payload), (long) PAYLOAD_LENGTH, "VARCHAR(MAX) [1MB]")
        .doFinally(signalType -> Arrays.fill(payload, '\0')); // Secure wipe
  }

  // --- CHARBUFFER TESTS ---

  private Mono<Void> testImplicitCharBufferBinding(ConnectionPool pool) {
    System.out.println("\n--- Test 5: Implicit CharBuffer Binding ---");
    char[] payload = new char[PAYLOAD_LENGTH];
    Arrays.fill(payload, 'A');
    CharBuffer buffer = CharBuffer.wrap(payload);

    return executeTest(pool, buffer, PAYLOAD_LENGTH * 2L, "NVARCHAR(MAX) [2MB]")
        .doFinally(signalType -> Arrays.fill(payload, '\0')); // Secure wipe
  }

  private Mono<Void> testExplicitCharBufferBinding(ConnectionPool pool) {
    System.out.println("\n--- Test 6: Explicit CharBuffer Binding (VARCHAR) ---");
    char[] payload = new char[PAYLOAD_LENGTH];
    Arrays.fill(payload, 'A');
    CharBuffer buffer = CharBuffer.wrap(payload);

    return executeTest(pool, Parameters.in(R2dbcType.VARCHAR, buffer), (long) PAYLOAD_LENGTH, "VARCHAR(MAX) [1MB]")
        .doFinally(signalType -> Arrays.fill(payload, '\0')); // Secure wipe
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
  /**
   * Throughput 3: Heavy Parameter Payload (VARCHAR MAX).
   * Binds a 1MB string EXPLICITLY as VARCHAR. The driver must respect the explicit
   * type and stream it as 1-byte ASCII, reporting exactly 1,048,576 bytes via DATALENGTH().
   */
  private Mono<Void> testMassiveVarCharBinding(ConnectionPool pool) {
    System.out.println("\n--- Test 3: Massive VARCHAR Payload Binding (1MB over wire) ---");
    int stringLength = 1024 * 1024; // 1 Megabyte string (1,048,576 chars)
    // 1. Generate the payload securely (No String Pool overhead)
    char[] massivePayload = new char[stringLength];
    java.util.Arrays.fill(massivePayload, 'A');

    return Mono.usingWhen(Mono.from(pool.create()),
        conn -> Flux.from(conn.createStatement("SELECT DATALENGTH(@payload) AS len")
                // EXPLICIT binding to force 1-byte encoding, even if it gets auto-promoted to Clob
                .bind("payload", Parameters.in(R2dbcType.VARCHAR, massivePayload)).execute())
            .flatMap(res -> res.map((row, meta) -> row.get(0, Long.class)))
            .doOnNext(len -> {
              System.out.println("  Server DATALENGTH() response: " + len + " bytes");
              if (len != (long) stringLength) {
                throw new IllegalStateException("Expected 1MB VARCHAR payload, but got " + len + " bytes");
              }
              System.out.println("  [OK] Successfully streamed as 1-byte VARCHAR(MAX)");
            })
            .then(),
        Connection::close);
  }

  // --- CORE EXECUTION ENGINE ---

  private Mono<Void> executeTest(ConnectionPool pool, Object boundValue, long expectedLength, String expectedTypeDesc) {
    return Mono.usingWhen(
        Mono.from(pool.create()),
        conn -> Flux.from(conn.createStatement("SELECT DATALENGTH(@payload) AS len")
                .bind("payload", boundValue)
                .execute())
            .flatMap(res -> res.map((row, meta) -> row.get(0, Long.class)))
            .doOnNext(len -> {
              System.out.println("  -> Server DATALENGTH() response: " + len + " bytes");
              if (len != expectedLength) {
                throw new IllegalStateException(String.format(
                    "Expected %s payload of %d bytes, but server reported %d bytes.",
                    expectedTypeDesc, expectedLength, len));
              }
              System.out.println("  ✓ [OK] Successfully streamed as " + expectedTypeDesc);
            })
            .then(),
        Connection::close
    );
  }
}