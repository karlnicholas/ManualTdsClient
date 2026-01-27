package com.example.flow;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;

public class IntegerPublisher implements Flow.Publisher<Integer> {
  @Override
  public void subscribe(Flow.Subscriber<? super Integer> subscriber) {
    subscriber.onSubscribe(new Flow.Subscription() {
      @Override
      public void request(long n) {
        ExecutorService threadPoolExecutor = Executors.newSingleThreadExecutor();
        threadPoolExecutor.submit(() -> {
          subscriber.onNext(1);
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
