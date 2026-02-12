package com.example.mappingproducer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.ForkJoinPool;

/**
 * The Main class implements an application that reads lines from the standard input
 * and prints them to the standard output.
 */
public class SimpleFlowPubSubThread {
  /**
   * Iterate through each line of input.
   */
  public static void main(String[] args) throws InterruptedException {
    new SimpleFlowPubSubThread().run();
  }

  private void run() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    new Publisher().subscribe(new Subscriber(latch));
    latch.await();
  }

  static class Publisher implements Flow.Publisher<Integer> {
    @Override
    public void subscribe(Flow.Subscriber<? super Integer> subscriber) {
      subscriber.onSubscribe(new Subscription(subscriber));
    }
  }

  static class Subscriber implements Flow.Subscriber<Integer> {
    private final CountDownLatch latch;
    Subscriber(CountDownLatch latch) {
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

   static class Subscription implements Flow.Subscription {
    private final ThreadTask threadTask;
     Subscription(Flow.Subscriber<? super Integer> subscriber) {
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