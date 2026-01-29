package com.example.flow2;

import java.util.concurrent.Flow;
import java.util.function.Function;

public class FlowResultWrapper<T> implements Flow.Publisher<T> {
  private final Flow.Publisher<? extends FlowResult> source;
  private final Function<Integer, ? extends T> mapper;

  private FlowResultWrapper(Flow.Publisher<? extends FlowResult> source, Function<Integer, ? extends T> mapper) {
    this.source = source;
    this.mapper = mapper;
  }

  // Static factory to start the chain
  public static FlowResultWrapper<Integer> wrap(Flow.Publisher<? extends FlowResult> source) {
    return new FlowResultWrapper<>(source, i -> i); // Default identity map
  }

  // Allows chaining: .map(i -> "Result: " + i)
  public <R> FlowResultWrapper<R> map(Function<T, ? extends R> nextMapper) {
    return new FlowResultWrapper<>(source, i -> nextMapper.apply(mapper.apply(i)));
  }

  @Override
  public void subscribe(Flow.Subscriber<? super T> subscriber) {
    source.subscribe(new Flow.Subscriber<FlowResult>() {
      @Override
      public void onNext(FlowResult result) {
        // Bridge to the ListByteArrayPublisher inside FlowResult
        result.map(mapper::apply).subscribe(subscriber);
      }
      @Override public void onSubscribe(Flow.Subscription s) { s.request(Long.MAX_VALUE); }
      @Override public void onError(Throwable t) { subscriber.onError(t); }
      @Override public void onComplete() { /* Inner publisher handles completion */ }
    });
  }
}