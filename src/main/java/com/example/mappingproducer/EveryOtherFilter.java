package com.example.mappingproducer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class EveryOtherFilter {

  public static void main(String[] args) throws InterruptedException {
    new EveryOtherFilter().run();
  }

  private void run() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);

    // Assembly remains the same - Decorator pattern in action
    new JustPublisher<>(10).everyOther().chain().everyOther()
        .subscribe(new ClientSubscriber<>(latch));

    latch.await();
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
    public Publisher<T> chain() {
      return new Publisher<>(this);
    }
    public EveryOtherPublisher<T> everyOther() {
      return new EveryOtherPublisher<>(this);
    }
  }

  static class EveryOtherPublisher<T> implements Flow.Publisher<T> {
    private final Flow.Publisher<T> source;
    EveryOtherPublisher(Flow.Publisher<T> source) { this.source = source; }
    @Override
    public void subscribe(Flow.Subscriber<? super T> subscriber) {
      source.subscribe(new EveryOtherSubscriber<>(subscriber));
    }
    public Publisher<T> chain() {
      return new Publisher<>(this);
    }
    public EveryOtherPublisher<T> everyOther() {
      return new EveryOtherPublisher<>(this);
    }
  }

  static class EveryOtherSubscriber<T> implements Flow.Subscriber<T> {
    private final Flow.Subscriber<? super T> downstream;
    private final AtomicLong counter = new AtomicLong(0);
    private Flow.Subscription upstreamSubscription; // Store the subscription

    EveryOtherSubscriber(Flow.Subscriber<? super T> downstream) {
      this.downstream = downstream;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      this.upstreamSubscription = subscription; // Save it
      downstream.onSubscribe(subscription);
    }

    @Override
    public void onNext(T item) {
      if (counter.getAndIncrement() % 2 == 0) {
        downstream.onNext(item);
      } else {
        // WE DROPPED ONE!
        // We must ask the upstream for a replacement so the Sink gets its full count.
        upstreamSubscription.request(1);
      }
    }

    @Override
    public void onError(Throwable throwable) {
      downstream.onError(throwable);
    }

    @Override
    public void onComplete() {
      downstream.onComplete();
    }
  }
  // =================================================================================
  // 2. THE API WRAPPER
  // =================================================================================
  static class Publisher<T> implements Flow.Publisher<T> {
    private final Flow.Publisher<T> source;
    public Publisher(Flow.Publisher<T> source) { this.source = source; }

    @Override
    public void subscribe(Flow.Subscriber<? super T> subscriber) { source.subscribe(subscriber); }

    public Publisher<T> chain() {
      return new Publisher<>(this);
    }
    public EveryOtherPublisher<T> everyOther() {
      return new EveryOtherPublisher<>(this);
    }
  }

  // =================================================================================
  // 4. THE SINK
  // =================================================================================
  static class ClientSubscriber<T> implements Flow.Subscriber<T> {
    private final CountDownLatch latch;
    ClientSubscriber(CountDownLatch latch) { this.latch = latch; }

    @Override
    public void onSubscribe(Flow.Subscription s) {
      // Requesting 10 results
      s.request(10);
    }

    @Override
    public void onNext(T item) { System.out.println("SINK RECEIVED: " + item); }

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