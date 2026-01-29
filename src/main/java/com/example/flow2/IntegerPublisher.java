package com.example.flow2;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;

public class IntegerPublisher implements Flow.Publisher<Integer> {
  private final List<Integer> list;

  public IntegerPublisher() {
    this.list = List.of(1,2,3);
  }
  @Override
  public void subscribe(Flow.Subscriber<? super Integer> subscriber) {
    subscriber.onSubscribe(new Flow.Subscription() {
      @Override
      public void request(long n) {
        ExecutorService threadPoolExecutor = Executors.newSingleThreadExecutor();
        threadPoolExecutor.submit(() -> {
          for(Integer i : list) {
            subscriber.onNext(i);
          }
            subscriber.onComplete();
        });
        threadPoolExecutor.shutdown();
      }

      @Override
      public void cancel() {

      }
    });
  }

}
