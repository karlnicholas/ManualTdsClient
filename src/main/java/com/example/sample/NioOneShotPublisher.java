package com.example.sample;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Flow;

class NioOneShotPublisher implements Flow.Publisher<ByteBuffer> {
  private final SocketChannel socketChannel;
  private final Selector selector;
  private boolean subscribed;

  public NioOneShotPublisher(SocketChannel socketChannel, Selector selector) {
    this.socketChannel = socketChannel;
    this.selector = selector;
  }

  public synchronized void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
    if (subscribed)
      subscriber.onError(new IllegalStateException("One-shot only!"));
    else {
      subscribed = true;
      subscriber.onSubscribe(new NioOneShotSubscription(subscriber, socketChannel, selector));
    }
  }

  static class NioOneShotSubscription implements Flow.Subscription {
    private final Flow.Subscriber<? super ByteBuffer> subscriber;
    private final SocketChannel channel;
    private final Selector selector;
    private SelectionKey key; // Replaces 'Future<?>'
    private boolean completed;

    NioOneShotSubscription(Flow.Subscriber<? super ByteBuffer> subscriber,
                           SocketChannel channel, Selector selector) {
      this.subscriber = subscriber;
      this.channel = channel;
      this.selector = selector;
    }

    public synchronized void request(long n) {
      if (!completed) {
        // In NIO, we don't "submit" work. We register "interest".
        try {
          // Best Practice: Register with the existing Selector
          // We wake up the selector to update interestOps safely
          selector.wakeup();
          key = channel.register(selector, SelectionKey.OP_READ, this);
        } catch (IOException e) {
          subscriber.onError(e);
        }
      }
    }

    public synchronized void cancel() {
      completed = true;
      // Best Practice: Cancel the key to stop monitoring the socket
      if (key != null) {
        key.cancel();
      }
    }

    // This method is called by the "Event Thread" (Selector Loop)
    public void onSocketReadable() {
      // Double-check state to handle race conditions (Best Practice)
      synchronized(this) {
        if (completed) return;
        completed = true;
      }

      try {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int read = channel.read(buffer);
        if (read == -1) {
          subscriber.onComplete(); // End of Stream
        } else {
          buffer.flip();
          subscriber.onNext(buffer);
          subscriber.onComplete(); // One-shot behavior
        }
      } catch (IOException ex) {
        subscriber.onError(ex);
      } finally {
        cancel(); // Cleanup key
      }
    }
  }
}