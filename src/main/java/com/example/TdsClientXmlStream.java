package com.example;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.api.TdsLibOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.stax.StAXSource;
import org.w3c.dom.Document;

import java.io.IOException;
import java.io.Reader;
import java.time.Duration;
import java.util.Iterator;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsClientXmlStream {

  private static final Logger logger = LoggerFactory.getLogger(TdsClientXmlStream.class);

  public static void main(String[] args) throws Exception {
    new TdsClientXmlStream().run();
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

    // Configure a simple pool for the standalone client execution
    ConnectionPoolConfiguration poolConfiguration = ConnectionPoolConfiguration.builder(connectionFactory)
        .initialSize(2)
        .maxSize(10)
        .maxIdleTime(Duration.ofMinutes(10))
        .build();

    ConnectionPool pool = new ConnectionPool(poolConfiguration);

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
    String massiveXmlSql = """
    SELECT CAST(
        '<root>' 
        + REPLICATE(CONVERT(varchar(max), '<dummy>padding</dummy>'), 100000) 
        + '<target><secretId>42</secretId><message>Reactive DOM Instantiation Successful!</message></target>' 
        + '</root>'
    AS xml)
    """;

    return Flux.from(connection.createStatement(massiveXmlSql).execute())
        .flatMap(result -> result.map((row, meta) -> row.get(0, io.r2dbc.spi.Clob.class)))
        .flatMap(clob -> {
          Flux<CharSequence> monitoredStream = Flux.from(clob.stream())
              .doOnRequest(n -> logger.trace("Driver requested {} chunks from socket", n))
              .doOnNext(chunk -> logger.trace("Driver emitted chunk ({} chars)", chunk.length()));

          System.out.println("\n--- Initiating Chunked XML Stream ---");

          Iterable<CharSequence> chunkIterable = monitoredStream.toIterable(1);

          return Mono.fromCallable(() -> parseAndFilterXml(chunkIterable))
              .subscribeOn(Schedulers.boundedElastic());
        })
        .doOnNext(document -> {
          if (document != null) {
            String message = document.getElementsByTagName("message").item(0).getTextContent();
            System.out.println("\n[RESULT] ✓ Successfully extracted DOM Document!");
            System.out.println("[RESULT] ✓ Message Value: " + message);
          }
        })
        .then();
  }

  private Document parseAndFilterXml(Iterable<CharSequence> chunks) throws Exception {
    logger.debug("StAX Thread starting...");
    Reader xmlReader = new FluxReader(chunks);

    XMLInputFactory factory = XMLInputFactory.newInstance();
    XMLStreamReader reader = factory.createXMLStreamReader(xmlReader);

    while (reader.hasNext()) {
      int event = reader.next();
      if (event == XMLStreamConstants.START_ELEMENT) {
        if ("target".equals(reader.getLocalName())) {
          logger.debug("Target tag found. Commencing DOM transformation...");

          TransformerFactory tf = TransformerFactory.newInstance();
          Transformer transformer = tf.newTransformer();
          DOMResult result = new DOMResult();
          transformer.transform(new StAXSource(reader), result);

          return (Document) result.getNode();
        }
      }
    }
    return null;
  }

  private static class FluxReader extends Reader {
    private final Iterator<CharSequence> iterator;
    private String currentChunk = "";
    private int index = 0;
    private int chunkCount = 0;

    public FluxReader(Iterable<CharSequence> chunks) {
      this.iterator = chunks.iterator();
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
      if (index >= currentChunk.length()) {
        logger.trace("Buffer empty. Requesting next chunk from Iterator...");

        if (!iterator.hasNext()) {
          logger.debug("Iterator reached EOF.");
          return -1;
        }

        CharSequence nextChunk = iterator.next();
        currentChunk = nextChunk != null ? nextChunk.toString() : "";
        index = 0;
        chunkCount++;

        logger.trace("Received chunk {}. Processing...", chunkCount);
      }

      int charsToCopy = Math.min(len, currentChunk.length() - index);
      currentChunk.getChars(index, index + charsToCopy, cbuf, off);

      index += charsToCopy;

      return charsToCopy;
    }

    @Override
    public void close() {
      logger.debug("FluxReader closed.");
    }
  }
}