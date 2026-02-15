package com.example.mappingproducer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class JustAndTwoClient {

  public static void main(String[] args) throws InterruptedException {
    new JustAndTwoClient().run();
  }

  private void run() throws InterruptedException {
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);

    // Assembly remains the same - Decorator pattern in action
    JustPublisher<Integer> pipe = new JustPublisher<>(10);
    pipe.subscribe(new ClientSubscriber<>(latch1, 5, "c1"));
    pipe.subscribe(new ClientSubscriber<>(latch2, 10, "c2"));

    latch1.await();
    latch2.await();
  }

  // =================================================================================
  // 1. THE GENERIC SOURCE
  // =================================================================================
  static class JustPublisher<T> implements Flow.Publisher<T> {
    private final T value;
    JustPublisher(T value) { this.value = value; }

    @Override
    public void subscribe(Flow.Subscriber<? super T> subscriber) {
      subscriber.onSubscribe(new JustSubscription<>(subscriber, value));
    }
  }

  // =================================================================================
  // 4. THE SINK
  // =================================================================================
  static class ClientSubscriber<T> implements Flow.Subscriber<T> {
    private final CountDownLatch latch;
    private final Integer count;
    private final String p;
    ClientSubscriber(CountDownLatch latch, Integer count, String p) {
      this.latch = latch;
      this.count = count;
      this.p = p;
    }

    @Override
    public void onSubscribe(Flow.Subscription s) {
      // Requesting 10 results
      s.request(count);
    }

    @Override
    public void onNext(T item) { System.out.println(p + "SINK RECEIVED: " + item); }

    @Override
    public void onError(Throwable t) { t.printStackTrace(); latch.countDown(); }

    @Override
    public void onComplete() {
      System.out.println("SINK: onComplete called");
      latch.countDown();
    }
  }

  // =================================================================================
  // THE CORRECTED WORKER LOGIC
  // =================================================================================
  static class JustSubscription<T> implements Flow.Subscription {
    private final Flow.Subscriber<? super T> subscriber;
    private final T value;
    private final AtomicLong demand = new AtomicLong();
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private volatile boolean cancelled = false;

    JustSubscription(Flow.Subscriber<? super T> subscriber, T value) {
      this.subscriber = subscriber;
      this.value = value;
    }

    @Override
    public void request(long n) {
      if (n <= 0) return;
      demand.addAndGet(n); // Track how many items are requested total
      // Use compareAndSet to ensure only one thread is pushing data at a time
      if (isRunning.compareAndSet(false, true)) {
        ForkJoinPool.commonPool().execute(this::drain);
      }
    }

    private void drain() {
      try {
        // Continue loop as long as there is demand and we aren't cancelled
        while (demand.get() > 0 && !cancelled) {
          subscriber.onNext(value);
          demand.decrementAndGet();
        }
        // Once the loop finishes, if we aren't cancelled, we complete
        if (!cancelled) {
          subscriber.onComplete();
        }
      } catch (Exception e) {
        subscriber.onError(e);
      } finally {
        isRunning.set(false);
      }
    }
    @Override
    public void cancel() { cancelled = true; }
  }
}