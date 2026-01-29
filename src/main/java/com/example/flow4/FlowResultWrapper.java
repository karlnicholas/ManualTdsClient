package com.example.flow4;

import java.util.concurrent.Flow;
import java.util.function.BiFunction;

public class FlowResultWrapper<T> implements Flow.Publisher<T> {
  private final Flow.Publisher<? extends FlowResult> source;
  private final BiFunction<FlowRow, FlowRowMetadata, ? extends T> mapper;

  // Make this constructor protected or package-private so the static methods can use it
  FlowResultWrapper(Flow.Publisher<? extends FlowResult> source, BiFunction<FlowRow, FlowRowMetadata, ? extends T> mapper) {
    this.source = source;
    this.mapper = mapper;
  }

  // 1. Existing method: Defaults to returning FlowRows (Identity)
  public static FlowResultWrapper<FlowRow> wrap(Flow.Publisher<? extends FlowResult> source) {
    return new FlowResultWrapper<>(source, (row, meta) -> row);
  }

  // 2. NEW METHOD: Mimics 'flatMapMany(result -> result.map(mapper))'
  // Takes the source AND the mapper in one step.
  public static <R> FlowResultWrapper<R> mapFrom(
      Flow.Publisher<? extends FlowResult> source,
      BiFunction<FlowRow, FlowRowMetadata, ? extends R> mapper) {
    return new FlowResultWrapper<>(source, mapper);
  }

  public <R> FlowResultWrapper<R> map(BiFunction<T, FlowRowMetadata, ? extends R> nextMapper) {
    return new FlowResultWrapper<>(source, (flowRow, flowRowMetadata) -> {
      T previousResult = this.mapper.apply(flowRow, flowRowMetadata);
      return nextMapper.apply(previousResult, flowRowMetadata);
    });
  }

  @Override
  public void subscribe(Flow.Subscriber<? super T> subscriber) {
    source.subscribe(new Flow.Subscriber<FlowResult>() {
      @Override
      public void onNext(FlowResult result) {
        result.map(mapper).subscribe(subscriber);
      }
      @Override public void onSubscribe(Flow.Subscription s) { s.request(Long.MAX_VALUE); }
      @Override public void onError(Throwable t) { subscriber.onError(t); }
      @Override public void onComplete() { /* Inner publisher handles completion */ }
    });
  }
}