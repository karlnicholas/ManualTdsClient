package com.example.sample;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

public class NioClientPublisher implements Flow.Publisher<ByteBuffer>, AutoCloseable {
  private final ExecutorService executor;
  private final AtomicBoolean running = new AtomicBoolean(true);

  // Shared persistent connection
  private final SocketChannel socketChannel;
  private final Selector selector;

  // We track the *current* subscriber to route the response to
  private Flow.Subscriber<? super ByteBuffer> currentSubscriber;
  private NioSubscription currentSubscription;

  public NioClientPublisher(int port) throws IOException {
    // 1. Connect during instantiation
    System.out.println("[Client] Connecting...");
    this.socketChannel = SocketChannel.open(new InetSocketAddress("localhost", port));
    this.socketChannel.configureBlocking(false);
    this.selector = Selector.open();

    // Register for reading (we always want to be ready to read responses)
    this.socketChannel.register(selector, SelectionKey.OP_READ);

    this.executor = Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r, "NioClient-EventLoop");
      t.setDaemon(true);
      return t;
    });

    // Start the loop immediately
    this.executor.submit(this::runEventLoop);
  }

  @Override
  public synchronized void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
    // For this simple demo, we only allow one active subscriber at a time
    // to keep the read/write logic clear (Sequential Re-use).
    if (currentSubscription != null && !currentSubscription.isCompleted()) {
      subscriber.onError(new IllegalStateException("Publisher is busy with another subscriber"));
      return;
    }

    currentSubscriber = subscriber;
    currentSubscription = new NioSubscription(subscriber, this);
    subscriber.onSubscribe(currentSubscription);
  }

  // Called by Subscription to send the "Trigger" request to the server
  void sendRequest() {
    try {
      // Write a dummy byte to trigger the server's response
      ByteBuffer trigger = ByteBuffer.wrap(new byte[]{1});
      while(trigger.hasRemaining()) {
        socketChannel.write(trigger);
      }
    } catch (IOException e) {
      if(currentSubscriber != null) currentSubscriber.onError(e);
    }
  }

  private void runEventLoop() {
    try {
      ByteBuffer buffer = ByteBuffer.allocate(1024);

      while (running.get() && !Thread.currentThread().isInterrupted()) {
        if (selector.select(100) == 0) continue;

        Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
        while (keys.hasNext()) {
          SelectionKey key = keys.next();
          keys.remove();

          if (key.isValid() && key.isReadable()) {
            buffer.clear();
            int read = socketChannel.read(buffer);
            if (read > 0) {
              buffer.flip();
              // Route data to the currently active subscriber
              if (currentSubscriber != null) {
                // Copy buffer for safety
                ByteBuffer msg = ByteBuffer.allocate(read);
                msg.put(buffer);
                msg.flip();
                currentSubscriber.onNext(msg);

                // For this demo: One Request = One Response, then Complete.
                // This resets the publisher for the next subscriber.
                currentSubscription.complete();
              }
            } else if (read == -1) {
              // Server closed connection
              // If we have a subscriber waiting, notify them of the error
              if (currentSubscriber != null) {
                currentSubscriber.onError(new java.io.EOFException("Server closed connection"));
              }
              close();
            }
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() {
    running.set(false);
    try {
      socketChannel.close();
      selector.close();
    } catch (IOException e) { /* ignore */ }
    executor.shutdownNow();
  }

  static class NioSubscription implements Flow.Subscription {
    private final Flow.Subscriber<? super ByteBuffer> subscriber;
    private final NioClientPublisher parent;
    private volatile boolean completed = false;

    NioSubscription(Flow.Subscriber<? super ByteBuffer> subscriber, NioClientPublisher parent) {
      this.subscriber = subscriber;
      this.parent = parent;
    }

    @Override
    public void request(long n) {
      if (!completed && n > 0) {
        // The request triggers the write to the server
        parent.sendRequest();
      }
    }

    @Override
    public void cancel() {
      completed = true;
    }

    void complete() {
      completed = true;
      subscriber.onComplete();
    }

    boolean isCompleted() { return completed; }
  }
}