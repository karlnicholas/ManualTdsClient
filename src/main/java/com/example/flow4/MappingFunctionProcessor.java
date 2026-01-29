package com.example.flow4;

import java.util.concurrent.Flow;
import java.util.function.BiFunction;

// A generic version of your IncrementProcessor logic
public class MappingFunctionProcessor<T extends FlowRow, R> implements Flow.Processor<T, R> {
  private final BiFunction<T, FlowRowMetadata, ? extends R>  mapper;
  private Flow.Subscriber<? super R> downstream;

  public MappingFunctionProcessor(BiFunction<T, FlowRowMetadata, ? extends R>  mapper) {
    this.mapper = mapper;
  }

  @Override
  public void subscribe(Flow.Subscriber<? super R> subscriber) {
    this.downstream = subscriber;
  }

  @Override
  public void onSubscribe(Flow.Subscription subscription) {
    downstream.onSubscribe(subscription);
  }

  @Override
  public void onNext(T flowRow) {
    downstream.onNext(mapper.apply(flowRow, flowRow.getFlowRowMetadata()));
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