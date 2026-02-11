package com.example.sample;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;

public class NioDemoAppOrig {
  public static void main(String[] args) throws Exception {
    int PORT = 9999;

    // 1. Start the simple Server in a separate thread
    Thread serverThread = new Thread(new NioServer(PORT));
    serverThread.start();

    // Give server a moment to start
    Thread.sleep(100);

    // 2. Setup the Client Channel (Non-blocking for Selector use)
    SocketChannel client = SocketChannel.open(new InetSocketAddress("localhost", PORT));
    client.configureBlocking(false);

    // We need a Selector because the Publisher requires one
    Selector selector = Selector.open();

    // 3. Create the Publisher
    // Passing the non-blocking channel and the selector we control
    NioOneShotPublisher publisher = new NioOneShotPublisher(client, selector);

    // 4. Create a Subscriber
    // We use a Latch to keep main thread alive until done
    CountDownLatch latch = new CountDownLatch(1);

    // Publisher emits ByteBuffer, so Subscriber must accept ByteBuffer
    publisher.subscribe(new Flow.Subscriber<ByteBuffer>() {
      @Override
      public void onSubscribe(Flow.Subscription subscription) {
        System.out.println("[Client] Subscribed. Requesting data...");
        subscription.request(1);
      }

      @Override
      public void onNext(ByteBuffer item) {
        // Decode ByteBuffer to String
        byte[] bytes = new byte[item.remaining()];
        item.get(bytes);
        String message = new String(bytes, StandardCharsets.UTF_8);
        System.out.println("[Client] Received: " + message);
      }

      @Override
      public void onError(Throwable throwable) {
        throwable.printStackTrace();
        latch.countDown();
      }

      @Override
      public void onComplete() {
        System.out.println("[Client] Done.");
        latch.countDown();
      }
    });

    // 5. Run the Selector Loop (The "Event Thread")
    // The Publisher registers the key, but WE must select and dispatch.
    while (latch.getCount() > 0) {
      // Wait for events with a small timeout so we can check the latch
      if (selector.select(100) == 0) {
        continue;
      }

      Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
      while (keys.hasNext()) {
        SelectionKey key = keys.next();
        keys.remove();

        if (key.isReadable()) {
          Object attachment = key.attachment();
          // Accessing the package-private inner class to drive the read
          if (attachment instanceof NioOneShotPublisher.NioOneShotSubscription) {
            ((NioOneShotPublisher.NioOneShotSubscription) attachment).onSocketReadable();
          }
        }
      }
    }

    // Cleanup
    if (client.isOpen()) client.close();
    selector.close();
  }
}