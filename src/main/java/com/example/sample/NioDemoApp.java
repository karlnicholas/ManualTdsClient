package com.example.sample;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;

public class NioDemoApp {
  public static void main(String[] args) throws Exception {
    int PORT = 9999;

    // 1. Start Server
    // We keep a reference to the server instance to stop it later
    NioServer server = new NioServer(PORT);
    Thread serverThread = new Thread(server);
    serverThread.start();
    Thread.sleep(500);

    // 2. Create Client Publisher
    try (NioClientPublisher publisher = new NioClientPublisher(PORT)) {

      // We will run 3 subscribers sequentially
      for (int i = 1; i <= 3; i++) {
        CountDownLatch latch = new CountDownLatch(1);
        String subName = "Subscriber-" + i;

        System.out.println("\n[App] Starting " + subName);

        publisher.subscribe(new Flow.Subscriber<>() {
          @Override
          public void onSubscribe(Flow.Subscription subscription) {
            System.out.println("[" + subName + "] Subscribed. Requesting data...");
            subscription.request(1);
          }

          @Override
          public void onNext(ByteBuffer item) {
            byte[] bytes = new byte[item.remaining()];
            item.get(bytes);
            String msg = new String(bytes, StandardCharsets.UTF_8);
            System.out.println("[" + subName + "] Received: " + msg);
          }

          @Override
          public void onError(Throwable throwable) {
            throwable.printStackTrace();
            latch.countDown();
          }

          @Override
          public void onComplete() {
            System.out.println("[" + subName + "] Finished.");
            latch.countDown();
          }
        });

        latch.await();
        Thread.sleep(200);
      }
    } // Publisher closes here automatically

    // 3. Shutdown Server
    System.out.println("\n[App] Shutting down server...");
    server.stop();
    serverThread.join(); // Wait for server thread to die
    System.out.println("[App] Done.");
  }
}