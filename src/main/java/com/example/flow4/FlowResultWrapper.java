package com.example.flow4;

import java.util.concurrent.Flow;
import java.util.function.BiFunction;

public class FlowResultWrapper<T> implements Flow.Publisher<T> {
  private final Flow.Publisher<? extends FlowResult> source;
  // This mapper turns (Row + Meta) -> T
  private final BiFunction<FlowRow, FlowRowMetadata, ? extends T> mapper;

  private FlowResultWrapper(Flow.Publisher<? extends FlowResult> source, BiFunction<FlowRow, FlowRowMetadata, ? extends T> mapper) {
    this.source = source;
    this.mapper = mapper;
  }

  public static FlowResultWrapper<FlowRow> wrap(Flow.Publisher<? extends FlowResult> source) {
    // Identity map: Returns the FlowRow itself
    return new FlowResultWrapper<>(source, (flowRow, flowRowMetadata) -> flowRow);
  }

  public <R> FlowResultWrapper<R> map(BiFunction<T, FlowRowMetadata, ? extends R> nextMapper) {
    return new FlowResultWrapper<>(source, (flowRow, flowRowMetadata) -> {
      // 1. Get the result of the CURRENT mapper (T)
      T previousResult = this.mapper.apply(flowRow, flowRowMetadata);
      // 2. Pass T and Metadata to the NEXT mapper
      R x = nextMapper.apply(previousResult, flowRowMetadata);
      return x;
    });
  }

  @Override
  public void subscribe(Flow.Subscriber<? super T> subscriber) {
    source.subscribe(new Flow.Subscriber<FlowResult>() {
      @Override
      public void onNext(FlowResult result) {
        // FIX: Directly pass the 'mapper' BiFunction.
        // FlowResult.map expects a BiFunction, and 'mapper' IS a BiFunction.
        // No lambda wrappers needed.
        result.map(mapper).subscribe(subscriber);
      }

      @Override public void onSubscribe(Flow.Subscription s) { s.request(Long.MAX_VALUE); }
      @Override public void onError(Throwable t) { subscriber.onError(t); }
      @Override public void onComplete() { /* Inner publisher handles completion */ }
    });
  }
}