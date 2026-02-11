package com.example.sample;

import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.Flow;
import java.util.concurrent.CountDownLatch;

public class NioDemoApp {
  public static void main(String[] args) throws Exception {
    int PORT = 9999;

    // 1. Start the simple Server in a separate thread
    Thread serverThread = new Thread(new NioOneShotServer(PORT));
    serverThread.start();

    // Give server a moment to start
    Thread.sleep(100);

    // 2. Setup the Client Channel (Async)
    AsynchronousSocketChannel client = AsynchronousSocketChannel.open();
    client.connect(new InetSocketAddress("localhost", PORT)).get(); // Wait for connection

    // 3. Create the Publisher
    NioOneShotPublisher publisher = new NioOneShotPublisher(client);

    // 4. Create a Subscriber
    // We use a Latch to keep main thread alive until done
    CountDownLatch latch = new CountDownLatch(1);

    publisher.subscribe(new Flow.Subscriber<String>() {
      @Override
      public void onSubscribe(Flow.Subscription subscription) {
        System.out.println("[Client] Subscribed. Requesting data...");
        subscription.request(1);
      }

      @Override
      public void onNext(String item) {
        System.out.println("[Client] Received: " + item);
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

    // 5. Wait for the async process to finish
    latch.await();

    // Cleanup
    if(client.isOpen()) client.close();
  }
}