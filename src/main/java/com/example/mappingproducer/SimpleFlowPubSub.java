package com.example.mappingproducer;

import java.util.concurrent.Flow;

/**
 * The Main class implements an application that reads lines from the standard input
 * and prints them to the standard output.
 */
public class SimpleFlowPubSub {
  /**
   * Iterate through each line of input.
   */
  public static void main(String[] args) throws InterruptedException {
    new SimpleFlowPubSub().run();
  }

  private void run() {
    new Publisher().subscribe(new Subscriber());
  }

  static class Publisher implements Flow.Publisher<Integer> {
    @Override
    public void subscribe(Flow.Subscriber<? super Integer> subscriber) {
      subscriber.onSubscribe(new Subscription(subscriber));
    }
  }

  static class Subscriber implements Flow.Subscriber<Integer> {
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
    }
    @Override
    public void onComplete() {
    }
   }

   static class Subscription implements Flow.Subscription {
    private final Flow.Subscriber<? super Integer> subscriber;
     Subscription(Flow.Subscriber<? super Integer> subscriber) {
       this.subscriber = subscriber;
     }
     @Override
     public void request(long n) {
      subscriber.onNext(1);
      subscriber.onComplete();
     }
     @Override
     public void cancel() {
     }
   }

  }