package com.example.mappingproducer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;

/**
 * The Main class implements an application that reads lines from the standard input
 * and prints them to the standard output.
 */
public class FlowPubSubMapPubSub {
  /**
   * Iterate through each line of input.
   */
  public static void main(String[] args) throws InterruptedException {
    new FlowPubSubMapPubSub().run();
  }

  private void run() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    new Publisher(new JustPublisher()).map(i -> i + 1).subscribe(new ClientSubscriber(latch));
    latch.await();
  }

  static class JustPublisher implements Flow.Publisher {
    @Override
    public void subscribe(Flow.Subscriber subscriber) {
      subscriber.onSubscribe(new JustSubscription(subscriber));
    }
  }

  static class Publisher implements Flow.Publisher<Integer> {
    private final Flow.Publisher<Integer> source;

    public Publisher(Flow.Publisher<Integer> source) {
      this.source = source;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super Integer> subscriber) {
      source.subscribe(subscriber);
    }
    public Publisher map(Function<Integer, Integer> mapper) {
      return new Publisher(new MapPublisher(source, mapper));
    }

  }

  static class ClientSubscriber implements Flow.Subscriber<Integer> {
    private final CountDownLatch latch;
    ClientSubscriber(CountDownLatch latch) {
      this.latch = latch;
    }
    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      subscription.request(1);
    }
    @Override
    public void onNext(Integer item) {
      System.out.println(item);
    }
    @Override
    public void onError(Throwable throwable) {
      latch.countDown();
    }
    @Override
    public void onComplete() {
      latch.countDown();
    }
   }

   static class MapPublisher implements Flow.Publisher<Integer> {
     private final Flow.Publisher<Integer> upstream;
     private final Function<Integer, Integer> mapper;

     MapPublisher(Flow.Publisher<Integer> upstream, Function<Integer, Integer> mapper) {
       this.upstream = upstream;
       this.mapper = mapper;
     }

     @Override
     public void subscribe(Flow.Subscriber<? super Integer> subscriber) {
      upstream.subscribe(new MapSubscriber(subscriber, mapper));
     }
   }

  static class MapSubscriber implements Flow.Subscriber<Integer> {
    private final Flow.Subscriber<? super Integer> downstream;
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

   static class JustSubscription implements Flow.Subscription {
    private final ThreadTask threadTask;
     JustSubscription(Flow.Subscriber<? super Integer> subscriber) {
       threadTask = new ThreadTask(subscriber);
     }
     @Override
     public void request(long n) {
       ForkJoinPool.commonPool().execute(threadTask);
     }
     @Override
     public void cancel() {
       threadTask.cancel();
     }
   }

   static class ThreadTask implements Runnable {
     private final Flow.Subscriber<? super Integer> subscriber;
     private volatile boolean cancelled = false;
     ThreadTask(Flow.Subscriber<? super Integer> subscriber) {
       this.subscriber = subscriber;
     }
     @Override
     public void run() {
       if (cancelled) return;
       subscriber.onNext(1);
       if (!cancelled) {
         subscriber.onComplete();
       }
     }
     public void cancel() {
      cancelled = true;
     }
   }
  }