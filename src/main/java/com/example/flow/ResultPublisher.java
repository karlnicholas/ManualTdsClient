package com.example.flow;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;

public class ResultPublisher implements Flow.Publisher<FlowResult> {
  @Override
  public void subscribe(Flow.Subscriber<? super FlowResult> subscriber) {
//    subscriber.onSubscribe(new Flow.Subscription() {
//      @Override
//      public void request(long n) {
//        ExecutorService threadPoolExecutor = Executors.newSingleThreadExecutor();
//        threadPoolExecutor.submit(() -> {
//          subscriber.onNext(1);
//          subscriber.onComplete();
//        });
//        threadPoolExecutor.shutdown();
//      }
//
//      @Override
//      public void cancel() {
//
//      }
//    });
  }

}
