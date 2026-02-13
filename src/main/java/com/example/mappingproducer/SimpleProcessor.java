package com.example.mappingproducer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class SimpleProcessor {

  public static void main(String[] args) throws InterruptedException {
    new SimpleProcessor().run();
  }

  private void run() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);

    JustPublisher<Integer> source = new JustPublisher<>(1);
    ItoSProcessor itoSProcessor = new ItoSProcessor();
    ClientSubscriber<String> sink = new ClientSubscriber<>(latch);

    // Order doesn't matter anymore because we SAVE the subscription now.
    source.subscribe(itoSProcessor);
    itoSProcessor.subscribe(sink);

    latch.await();
  }

  // =================================================================================
  // PROCESSOR 1: Integer -> String
  // =================================================================================
  static class ItoSProcessor implements Flow.Processor<Integer, String> {

    private Flow.Subscriber<? super String> downstream;
    private Flow.Subscription upstream; // <--- ADDED BACK (Essential State)

    @Override
    public void subscribe(Flow.Subscriber<? super String> subscriber) {
      this.downstream = subscriber;
      // If the Source already called us, pass the phone number down now!
      if (this.upstream != null) {
        subscriber.onSubscribe(this.upstream);
      }
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      this.upstream = subscription; // <--- SAVE IT
      // If the Downstream is already waiting, pass the phone number down now!
      if (this.downstream != null) {
        downstream.onSubscribe(subscription);
      }
    }

    @Override
    public void onNext(Integer item) {
      // TRANSFORM & PASS
      if (downstream != null) downstream.onNext("One");
    }

    // Pass-through error/complete signals
    @Override public void onError(Throwable t) { if(downstream!=null) downstream.onError(t); }
    @Override public void onComplete() { if(downstream!=null) downstream.onComplete(); }
  }

  // =================================================================================
  // SOURCE (Same as before)
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
  // SINK (Same as before)
  // =================================================================================
  static class ClientSubscriber<T> implements Flow.Subscriber<T> {
    private final CountDownLatch latch;
    ClientSubscriber(CountDownLatch latch) { this.latch = latch; }
    @Override
    public void onSubscribe(Flow.Subscription s) { s.request(1); }
    @Override
    public void onNext(T item) { System.out.println("SINK RECEIVED: " + item); }
    @Override
    public void onError(Throwable t) { t.printStackTrace(); latch.countDown(); }
    @Override
    public void onComplete() { System.out.println("SINK: Done"); latch.countDown(); }
  }

  // =================================================================================
  // WORKER (Same as before)
  // =================================================================================
  static class JustSubscription<T> implements Flow.Subscription {
    private final Flow.Subscriber<? super T> subscriber;
    private final T value;
    private final AtomicLong demand = new AtomicLong();
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    JustSubscription(Flow.Subscriber<? super T> subscriber, T value) {
      this.subscriber = subscriber;
      this.value = value;
    }

    @Override
    public void request(long n) {
      if (n <= 0) return;
      demand.addAndGet(n);
      if (isRunning.compareAndSet(false, true)) ForkJoinPool.commonPool().execute(this::drain);
    }

    private void drain() {
      try {
        if (demand.get() > 0) {
          subscriber.onNext(value);
          demand.decrementAndGet();
          subscriber.onComplete();
        }
      } finally { isRunning.set(false); }
    }
    @Override public void cancel() { }
  }
}