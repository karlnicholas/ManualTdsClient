package com.example.flow;

import java.util.concurrent.Flow;
import java.util.function.Function;

// A generic version of your IncrementProcessor logic
public class GenericMapProcessor<T, R> implements Flow.Processor<T, R> {
  private final Function<? super T, ? extends R> mapper;
  private Flow.Subscriber<? super R> downstream;

  public GenericMapProcessor(Function<? super T, ? extends R> mapper) {
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
  public void onNext(T item) {
    downstream.onNext(mapper.apply(item));
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