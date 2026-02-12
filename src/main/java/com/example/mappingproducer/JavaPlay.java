package com.example.mappingproducer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.function.Function;

/**
 * The Main class implements an application that reads lines from the standard input
 * and prints them to the standard output.
 */
public class JavaPlay {
  /**
   * Iterate through each line of input.
   */
  public static void main(String[] args) throws InterruptedException {
    new JavaPlay().run3();
  }

  private void run3() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    MapPub.just(1).map(i->i+1).map(i->i+1).subscribe(new Flow.Subscriber<Integer>() {
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
        throwable.printStackTrace();
        latch.countDown();
      }

      @Override
      public void onComplete() {
        latch.countDown();
      }
    });
    latch.await();
  }


  static class MapPub implements Flow.Publisher<Integer> {
    private final Flow.Publisher<Integer> source;
    private Flow.Subscriber<? super Integer> downstream;

    MapPub(Flow.Publisher<Integer> source) {
      this.source = source;
      this.downstream = new JustSubscriber();
    }

    @Override
    public void subscribe(Flow.Subscriber<? super Integer> subscriber) {
      this.downstream = subscriber;
      source.subscribe(subscriber);
    }

    static MapPub just(Integer value) {
      return new MapPub(new JustPublisher(value));
    }

    static class JustPublisher implements Flow.Publisher<Integer> {
      private final Integer value;
      public JustPublisher(Integer value) {
        this.value = value;
      }

      @Override
      public void subscribe(Flow.Subscriber<? super Integer> subscriber) {
        subscriber.onSubscribe(new JustSubscription(subscriber, value));
      }
    }

    static class JustSubscriber implements Flow.Subscriber<Integer> {
      Flow.Subscriber<Integer> downstream;

      @Override
      public void onSubscribe(Flow.Subscription subscription) {
        downstream.onSubscribe(subscription);
      }

      @Override
      public void onNext(Integer item) {
        downstream.onNext(item);
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
      private final Integer value;
      private final Flow.Subscriber<? super Integer> subscriber;
      private Thread thread;

      public JustSubscription(Flow.Subscriber<? super Integer> subscriber, Integer value) {
        this.subscriber = subscriber;
        this.value = value;
      }

      @Override
      public void request(long n) {
        thread = new Thread(new JustTask(subscriber, value));
        thread.start();
      }

      @Override
      public void cancel() {
        thread.interrupt();
        try {
          thread.join();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        subscriber.onComplete();
      }
    }

    static class JustTask implements Runnable{
      private final Flow.Subscriber<? super Integer> subscriber;
      private final Integer value;


      JustTask(Flow.Subscriber<? super Integer> subscriber, Integer value) {
        this.subscriber = subscriber;
        this.value = value;
      }

      @Override
      public void run() {
        subscriber.onNext(value);
        subscriber.onComplete();
      }
    }

    public MapPub map(Function<Integer, Integer> mapper) {
      MapProd mapProd = new MapProd(this, mapper);
      return new MapPub(mapProd);
    }

    static class MapProd implements Flow.Publisher<Integer> {
      private final Flow.Publisher<Integer> source;
      private final Function<Integer, Integer> mapper;

      MapProd(Flow.Publisher<Integer> source, Function<Integer, Integer> mapper) {
        this.source = source;
        this.mapper = mapper;
      }

      @Override
      public void subscribe(Flow.Subscriber<? super Integer> subscriber) {
        source.subscribe(new MapSub(subscriber, mapper));
      }
    }

    static class MapSub implements Flow.Subscriber<Integer> {
      private final Flow.Subscriber<? super Integer> downstream;
      private final Function<Integer, Integer> mapper;

      MapSub(Flow.Subscriber<? super Integer> downstream, Function<Integer, Integer> mapper) {
        this.downstream = downstream;
        this.mapper = mapper;
      }

      @Override
      public void onSubscribe(Flow.Subscription subscription) {
        downstream.onSubscribe(subscription);
      }

      @Override
      public void onNext(Integer item) {
        Integer nextItem = mapper.apply(item);
        downstream.onNext(nextItem);
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
  }

  private void run2() {

    System.out.println("Main Thread Start: " + Thread.currentThread().getName());

    MappingProducer.just(1)
        .map(i -> i + 1)
        .map(i -> i + 1)
        .subscribe(i -> System.out.println("Value: " + i));

    System.out.println("Main Thread End");

  }

//  private void run() throws InterruptedException {
//    IntegerPublisher publisher = new IntegerPublisher();
//
//    MappingProducer.from(publisher).map(i->i+1).map(i->i+1).subscribe(System.out::println);
//
//    }

}