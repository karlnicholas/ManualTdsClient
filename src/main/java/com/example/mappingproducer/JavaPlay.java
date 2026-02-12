package com.example.mappingproducer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;

public class JavaPlay {

  public static void main(String[] args) throws InterruptedException {
    new JavaPlay().run();
  }

  private void run() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);

    // Now we can start with a String, an Integer, or anything else.
    // The types are handled automatically by the Decorator chain.
    new Publisher<>(new JustPublisher<>(10)) // Start with Integer 10
        .map(i -> i * 2)                     // Integer -> Integer (20)
        .map(i -> "Result: " + i)            // Integer -> String ("Result: 20")
        .subscribe(new ClientSubscriber<>(latch));

    latch.await();
  }

  // =================================================================================
  // 1. THE GENERIC SOURCE (No more <Integer>!)
  // =================================================================================
  static class JustPublisher<T> implements Flow.Publisher<T> {
    private final T value;

    JustPublisher(T value) {
      this.value = value;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super T> subscriber) {
      // The Subscription is now typed to T
      subscriber.onSubscribe(new JustSubscription<>(subscriber, value));
    }
  }

  // =================================================================================
  // 2. THE API WRAPPER
  // =================================================================================
  static class Publisher<T> implements Flow.Publisher<T> {
    private final Flow.Publisher<T> source;

    public Publisher(Flow.Publisher<T> source) {
      this.source = source;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super T> subscriber) {
      source.subscribe(subscriber);
    }

    public <R> Publisher<R> map(Function<T, R> mapper) {
      return new Publisher<>(new MapPublisher<>(this.source, mapper));
    }
  }

  // =================================================================================
  // 3. THE DECORATORS (Type Tunneling: T -> R)
  // =================================================================================
  static class MapPublisher<T, R> implements Flow.Publisher<R> {
    private final Flow.Publisher<T> upstream;
    private final Function<T, R> mapper;

    MapPublisher(Flow.Publisher<T> upstream, Function<T, R> mapper) {
      this.upstream = upstream;
      this.mapper = mapper;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super R> subscriber) {
      upstream.subscribe(new MapSubscriber<>(subscriber, mapper));
    }
  }

  static class MapSubscriber<T, R> implements Flow.Subscriber<T> {
    private final Flow.Subscriber<? super R> downstream;
    private final Function<T, R> mapper;

    MapSubscriber(Flow.Subscriber<? super R> downstream, Function<T, R> mapper) {
      this.downstream = downstream;
      this.mapper = mapper;
    }

    @Override
    public void onSubscribe(Flow.Subscription s) { downstream.onSubscribe(s); }
    @Override
    public void onNext(T item) { downstream.onNext(mapper.apply(item)); }
    @Override
    public void onError(Throwable t) { downstream.onError(t); }
    @Override
    public void onComplete() { downstream.onComplete(); }
  }

  // =================================================================================
  // 4. THE SINK
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
    public void onComplete() { latch.countDown(); }
  }

  // =================================================================================
  // 5. THE GENERIC WORKER
  // =================================================================================
  static class JustSubscription<T> implements Flow.Subscription {
    private final ThreadTask<T> threadTask;

    JustSubscription(Flow.Subscriber<? super T> subscriber, T value) {
      threadTask = new ThreadTask<>(subscriber, value);
    }

    @Override
    public void request(long n) {
      ForkJoinPool.commonPool().execute(threadTask);
    }

    @Override
    public void cancel() { threadTask.cancel(); }
  }

  static class ThreadTask<T> implements Runnable {
    private final Flow.Subscriber<? super T> subscriber;
    private final T value;
    private volatile boolean cancelled = false;

    ThreadTask(Flow.Subscriber<? super T> subscriber, T value) {
      this.subscriber = subscriber;
      this.value = value;
    }

    @Override
    public void run() {
      if (cancelled) return;
      subscriber.onNext(value);
      if (!cancelled) subscriber.onComplete();
    }

    public void cancel() { cancelled = true; }
  }
}