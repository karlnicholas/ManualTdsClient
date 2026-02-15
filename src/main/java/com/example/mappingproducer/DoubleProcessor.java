package com.example.mappingproducer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class DoubleProcessor {

  public static void main(String[] args) throws InterruptedException {
    new DoubleProcessor().run();
  }

  private void run() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);

    JustPublisher<Integer> source = new JustPublisher<>(1);
    ItoSProcessor itoSProcessor = new ItoSProcessor();
    StoIProcessor stoIProcessor = new StoIProcessor();
    ClientSubscriber<Integer> sink = new ClientSubscriber<>(latch);

    // Wire them up
    source.subscribe(itoSProcessor);
    itoSProcessor.subscribe(stoIProcessor);
    stoIProcessor.subscribe(sink);

    latch.await();
  }

  // =================================================================================
  // PROCESSOR 1: Integer -> String
  // =================================================================================
  static class ItoSProcessor implements Flow.Processor<Integer, String> {

    private Flow.Subscriber<? super String> downstreamSubscriber;
    private Flow.Subscription upstreamSubscription; // <--- RENAMED for clarity

    @Override
    public void subscribe(Flow.Subscriber<? super String> subscriber) {
      this.downstreamSubscriber = subscriber;
      // If we already have the subscription handle, pass it down immediately
      if (this.upstreamSubscription != null) {
        subscriber.onSubscribe(this.upstreamSubscription);
      }
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      this.upstreamSubscription = subscription; // Save the handle
      // If the subscriber is already waiting, pass the handle to them
      if (this.downstreamSubscriber != null) {
        downstreamSubscriber.onSubscribe(subscription);
      }
    }

    @Override
    public void onNext(Integer item) {
      if (downstreamSubscriber != null) downstreamSubscriber.onNext("One");
    }

    @Override public void onError(Throwable t) {
      if(downstreamSubscriber!=null) downstreamSubscriber.onError(t);
    }
    @Override public void onComplete() {
      if(downstreamSubscriber!=null) downstreamSubscriber.onComplete();
    }
  }

  // =================================================================================
  // PROCESSOR 2: String -> Integer
  // =================================================================================
  static class StoIProcessor implements Flow.Processor<String, Integer> {

    private Flow.Subscriber<? super Integer> downstreamSubscriber;
    private Flow.Subscription upstreamSubscription; // <--- RENAMED for clarity

    @Override
    public void subscribe(Flow.Subscriber<? super Integer> subscriber) {
      this.downstreamSubscriber = subscriber;
      if (this.upstreamSubscription != null) {
        subscriber.onSubscribe(this.upstreamSubscription);
      }
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      this.upstreamSubscription = subscription;
      if (this.downstreamSubscriber != null) {
        downstreamSubscriber.onSubscribe(subscription);
      }
    }

    @Override
    public void onNext(String item) {
      if (downstreamSubscriber != null) downstreamSubscriber.onNext(1);
    }

    @Override public void onError(Throwable t) {
      if(downstreamSubscriber!=null) downstreamSubscriber.onError(t);
    }
    @Override public void onComplete() {
      if(downstreamSubscriber!=null) downstreamSubscriber.onComplete();
    }
  }

  // =================================================================================
  // SOURCE (Unchanged)
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
  // SINK (Unchanged)
  // =================================================================================
  static class ClientSubscriber<T> implements Flow.Subscriber<T> {
    private final CountDownLatch latch;
    ClientSubscriber(CountDownLatch latch) { this.latch = latch; }
    @Override
    public void onSubscribe(Flow.Subscription s) { s.request(10); }
    @Override
    public void onNext(T item) { System.out.println("SINK RECEIVED: " + item); }
    @Override
    public void onError(Throwable t) { t.printStackTrace(); latch.countDown(); }
    @Override
    public void onComplete() { System.out.println("SINK: Done"); latch.countDown(); }
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