package com.example.mappingproducer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class IntegerMappingProducer {

  public static void main(String[] args) throws InterruptedException {
    new IntegerMappingProducer().run();
  }

  private void run() throws InterruptedException {
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);

    // Assembly remains the same - Decorator pattern in action
    MappingPublisher pipe = new MappingPublisher(new RepeatPublisher(10));
    pipe.map(i->i+1).subscribe(new ClientSubscriber<>(latch1, 5L, "c1"));
    pipe.map(i->i+2).subscribe(new ClientSubscriber<>(latch2, 10L, "c2"));

    latch1.await();
    latch2.await();
  }

  // =================================================================================
  // 1. THE GENERIC SOURCE
  // =================================================================================
  static class RepeatPublisher implements Flow.Publisher<Integer> {
    private final Integer value;
    RepeatPublisher(Integer value) { this.value = value; }

    @Override
    public void subscribe(Flow.Subscriber<? super Integer> subscriber) {
      subscriber.onSubscribe(new RepeatSubscription<>(subscriber, value));
    }
  }

  // =================================================================================
  // 4. THE SINK
  // =================================================================================
  static class ClientSubscriber<Integer> implements Flow.Subscriber<Integer> {
    private final CountDownLatch latch;
    private final Long count;
    private final String p;
    ClientSubscriber(CountDownLatch latch, Long count, String p) {
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
    public void onNext(Integer item) { System.out.println(p + "SINK RECEIVED: " + item); }

    @Override
    public void onError(Throwable t) { t.printStackTrace(); latch.countDown(); }

    @Override
    public void onComplete() {
      System.out.println("SINK: onComplete called");
      latch.countDown();
    }
  }
  // =================================================================================
  // MAPPING PUSBLISHER AND SUBSCRIBER
  // =================================================================================
  static class MappingPublisher implements Flow.Publisher<Integer> {
    private final Flow.Publisher<Integer> source;
    private Function<Integer, Integer> mapper;


    MappingPublisher(Flow.Publisher<Integer> source, Function<Integer, Integer> mapper) {
      this.source = source;
      this.mapper = mapper;
    }

    MappingPublisher(Flow.Publisher<Integer> source) {
      this.source = source;
      this.mapper = null;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super Integer> subscriber) {
      if ( mapper == null)
        source.subscribe(subscriber);
      else
        source.subscribe(new MapSubscriber(subscriber, mapper));
    }

    public MappingPublisher map(Function<Integer, Integer> mapper) {
      return new MappingPublisher(this, mapper);
    }
  }

  static class MapSubscriber implements Flow.Subscriber<Integer> {
    private final Flow.Subscriber <? super Integer> downstream;
    private final Function<Integer, Integer> mapper;

    MapSubscriber(Flow.Subscriber<? super Integer> downstream, Function<Integer, Integer> mapper) {
      this.downstream = downstream;
      this.mapper = mapper;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      downstream.onSubscribe(subscription);
    }

    @Override
    public void onNext(Integer item) {
      downstream.onNext(mapper.apply(item));
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
  // THE CORRECTED WORKER LOGIC
  // =================================================================================
  static class RepeatSubscription<Integer> implements Flow.Subscription {
    private final Flow.Subscriber <Integer> subscriber;
    private final Integer value;
    private final AtomicLong demand = new AtomicLong();
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private volatile boolean cancelled = false;

    RepeatSubscription(Flow.Subscriber <Integer> subscriber, Integer value) {
      this.subscriber = subscriber;
      this.value = value;
    }

    @Override
    public void request(long n) {
      if (n <= 0) return;
      demand.addAndGet(n); // Track how many items are requested total
      // Use compareAndSeInteger to ensure only one thread is pushing data aInteger a time
      if (isRunning.compareAndSet(false, true)) {
        ForkJoinPool.commonPool().execute(this::drain);
      }
    }

    private void drain() {
      try {
        // Continue loop as long as there is demand and we aren'Integer cancelled
        while (demand.get() > 0 && !cancelled) {
          subscriber.onNext(value);
          demand.decrementAndGet();
        }
        // Once the loop finishes, if we aren'Intege cancelled, we complete
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