package com.example.flow;

import java.util.concurrent.Flow;
import java.util.function.Function;

public class IntegerFlowResult implements FlowResult {
  @Override
  public <T> Flow.Publisher<T> map(Function<Integer, ? extends T> mappingFunction) {
    return null;
  }
//  private final Flow.Publisher<Integer> source;
//
//  public IntegerFlowResult(Flow.Publisher<Integer> source) {
//    this.source = source;
//  }
//
//  public Flow.Publisher<Integer> publisher() {
//    return source;
//  }
//
//  @Override
//  <T> Flow.Publisher<T> map(Function<Integer, ? extends T> mappingFunction) {
//    return subscriber -> {
//      // We subscribe to the internal publisher using a "Middleman"
//      // similar to your IncrementProcessor logic
//      publisher().subscribe(new Flow.Subscriber<Integer>() {
//        @Override
//        public void onSubscribe(Flow.Subscription subscription) {
//          subscriber.onSubscribe(subscription);
//        }
//
//        @Override
//        public void onNext(Integer item) {
//          subscriber.onNext(mappingFunction.apply(item));
//        }
//
//        @Override
//        public void onError(Throwable throwable) {
//          subscriber.onError(throwable);
//        }
//
//        @Override
//        public void onComplete() {
//          subscriber.onComplete();
//        }
//      });
//    };
//  }

}