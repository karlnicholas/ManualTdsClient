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

public class TdsClientBindChar {

  // 4000 chars is the boundary:
  // NVARCHAR(4000) = 8000 bytes (Max before PLP)
  // VARCHAR(4000) = 4000 bytes (Well under 8000 byte limit)
  private static final int PAYLOAD_LENGTH = 4000;

  public static void main(String[] args) {
    new TdsClientBindChar().run();
  }

  private void run() {
    String r2dbcUrl = "r2dbc:mssql://reactnonreact:reactnonreact@localhost:1433/reactnonreact?trustServerCertificate=true";
    ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(ConnectionFactories.get(r2dbcUrl)).initialSize(2).maxSize(50)
.build());

    System.out.println("Connecting to database pool for Standard Parameter Egress Testing...");

    Mono.usingWhen(Mono.just(pool), this::runSql, ConnectionPool::disposeLater)
        .doOnError(t -> System.err.println("\n❌ Test Suite Failed: " + t.getMessage()))
        .block();
  }

  public Mono<Void> runSql(ConnectionPool pool) {
    return Mono.defer(() -> testImplicitStringBinding(pool))
//        .then(Mono.defer(() -> testExplicitStringBinding(pool)))
//        .then(Mono.defer(() -> testImplicitCharArrayBinding(pool)))
//        .then(Mono.defer(() -> testExplicitCharArrayBinding(pool)))
//        .then(Mono.defer(() -> testImplicitCharBufferBinding(pool)))
//        .then(Mono.defer(() -> testExplicitCharBufferBinding(pool)))
        ;
  }

  // --- STRING TESTS ---

  private Mono<Void> testImplicitStringBinding(ConnectionPool pool) {
    System.out.println("\n--- Test 1: Implicit String Binding ---");
    String payload = "A".repeat(PAYLOAD_LENGTH);
    return executeTest(pool, payload, PAYLOAD_LENGTH * 2L, "NVARCHAR(4000) [8KB]");
  }

  private Mono<Void> testExplicitStringBinding(ConnectionPool pool) {
    System.out.println("\n--- Test 2: Explicit String Binding (VARCHAR) ---");
    String payload = "A".repeat(PAYLOAD_LENGTH);
    return executeTest(pool, Parameters.in(R2dbcType.VARCHAR, payload), (long) PAYLOAD_LENGTH, "VARCHAR(4000) [4KB]");
  }

  // --- CHAR ARRAY TESTS ---

  private Mono<Void> testImplicitCharArrayBinding(ConnectionPool pool) {
    System.out.println("\n--- Test 3: Implicit char[] Binding ---");
    char[] payload = new char[PAYLOAD_LENGTH];
    Arrays.fill(payload, 'A');

    return executeTest(pool, payload, PAYLOAD_LENGTH * 2L, "NVARCHAR(4000) [8KB]")
        .doFinally(signalType -> Arrays.fill(payload, '\0')); // Secure wipe
  }

  private Mono<Void> testExplicitCharArrayBinding(ConnectionPool pool) {
    System.out.println("\n--- Test 4: Explicit char[] Binding (VARCHAR) ---");
    char[] payload = new char[PAYLOAD_LENGTH];
    Arrays.fill(payload, 'A');

    return executeTest(pool, Parameters.in(R2dbcType.VARCHAR, payload), (long) PAYLOAD_LENGTH, "VARCHAR(4000) [4KB]")
        .doFinally(signalType -> Arrays.fill(payload, '\0')); // Secure wipe
  }

  // --- CHARBUFFER TESTS ---

  private Mono<Void> testImplicitCharBufferBinding(ConnectionPool pool) {
    System.out.println("\n--- Test 5: Implicit CharBuffer Binding ---");
    char[] payload = new char[PAYLOAD_LENGTH];
    Arrays.fill(payload, 'A');
    CharBuffer buffer = CharBuffer.wrap(payload);

    return executeTest(pool, buffer, PAYLOAD_LENGTH * 2L, "NVARCHAR(4000) [8KB]")
        .doFinally(signalType -> Arrays.fill(payload, '\0')); // Secure wipe
  }

  private Mono<Void> testExplicitCharBufferBinding(ConnectionPool pool) {
    System.out.println("\n--- Test 6: Explicit CharBuffer Binding (VARCHAR) ---");
    char[] payload = new char[PAYLOAD_LENGTH];
    Arrays.fill(payload, 'A');
    CharBuffer buffer = CharBuffer.wrap(payload);

    return executeTest(pool, Parameters.in(R2dbcType.VARCHAR, buffer), (long) PAYLOAD_LENGTH, "VARCHAR(4000) [4KB]")
        .doFinally(signalType -> Arrays.fill(payload, '\0')); // Secure wipe
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
              System.out.println("  ✓ [OK] Successfully bound as " + expectedTypeDesc);
            })
            .then(),
        Connection::close
    );
  }
}