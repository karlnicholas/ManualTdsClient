package com.example.flow4;

import java.util.concurrent.Flow;
import java.util.function.Function;

public class IntegerFlowResult implements FlowResult {
  @Override
  public <T> Flow.Publisher<T> map(Function<Integer, ? extends T> mappingFunction) {
    return subscriber -> {
      IntegerPublisher source = new IntegerPublisher();
      // We need a middle-man (Processor) to apply the mapping function
      source.subscribe(new MappingSubscriber<>(subscriber, mappingFunction));
    };
  }

  @Override
  public <T> Flow.Publisher<T> flatMap(Function<Integer, ? extends Flow.Publisher<? extends T>> mappingFunction) {
    return subscriber -> {
      IntegerPublisher source = new IntegerPublisher();
      source.subscribe(new Flow.Subscriber<Integer>() {
        @Override
        public void onSubscribe(Flow.Subscription s) { s.request(Long.MAX_VALUE); }

        @Override
        public void onNext(Integer item) {
          // For every integer, we get a NEW publisher
          Flow.Publisher<? extends T> innerPublisher = mappingFunction.apply(item);
          // We subscribe the downstream to this inner publisher
          innerPublisher.subscribe(new Flow.Subscriber<T>() {
            @Override public void onNext(T t) { subscriber.onNext(t); }
            @Override public void onSubscribe(Flow.Subscription s) { s.request(Long.MAX_VALUE); }
            @Override public void onError(Throwable t) { subscriber.onError(t); }
            @Override public void onComplete() { /* Inner completion handled */ }
          });
        }

        @Override public void onError(Throwable t) { subscriber.onError(t); }
        @Override public void onComplete() { subscriber.onComplete(); }
      });
    };
  }
}
