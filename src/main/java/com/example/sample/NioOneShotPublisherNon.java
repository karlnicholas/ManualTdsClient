package com.example.sample;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Flow;

// 1. Changed "implements Flow.Publisher<ByteBuffer>" to "<String>"
public class NioOneShotPublisherNon implements Flow.Publisher<String> {

  // 2. Changed "SocketChannel + Selector" to "AsynchronousSocketChannel"
  private final AsynchronousSocketChannel clientChannel;
  private boolean subscribed;

  // 3. Constructor now matches what your Main App is passing
  public NioOneShotPublisherNon(AsynchronousSocketChannel clientChannel) {
    this.clientChannel = clientChannel;
  }

  @Override
  public synchronized void subscribe(Flow.Subscriber<? super String> subscriber) {
    if (subscribed) {
      subscriber.onError(new IllegalStateException("One-shot publisher allows only one subscriber"));
    } else {
      subscribed = true;
      subscriber.onSubscribe(new NioOneShotSubscription(subscriber, clientChannel));
    }
  }

  static class NioOneShotSubscription implements Flow.Subscription {
    private final Flow.Subscriber<? super String> subscriber;
    private final AsynchronousSocketChannel channel;
    private boolean completed;

    NioOneShotSubscription(Flow.Subscriber<? super String> subscriber, AsynchronousSocketChannel channel) {
      this.subscriber = subscriber;
      this.channel = channel;
    }

    @Override
    public synchronized void request(long n) {
      if (!completed) {
        completed = true;

        if (n <= 0) {
          subscriber.onError(new IllegalArgumentException());
          return;
        }

        ByteBuffer buffer = ByteBuffer.allocate(1024);

        // 4. Using the Async read method with a CompletionHandler
        channel.read(buffer, buffer, new CompletionHandler<Integer, ByteBuffer>() {
          @Override
          public void completed(Integer result, ByteBuffer buf) {
            synchronized (NioOneShotSubscription.this) {
              if (!channel.isOpen()) return;
            }

            if (result == -1) {
              subscriber.onComplete();
            } else {
              buf.flip();
              // 5. Decoding the bytes to String before sending to Subscriber
              String msg = StandardCharsets.UTF_8.decode(buf).toString();
              subscriber.onNext(msg);
              subscriber.onComplete();
            }
          }

          @Override
          public void failed(Throwable exc, ByteBuffer buf) {
            synchronized (NioOneShotSubscription.this) {
              if (!channel.isOpen()) return;
            }
            subscriber.onError(exc);
          }
        });
      }
    }

    @Override
    public synchronized void cancel() {
      completed = true;
      try {
        channel.close();
      } catch (Exception e) {
        // Ignore close errors
      }
    }
  }
}