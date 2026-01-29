package com.example.flow4;

import java.util.concurrent.Flow;
import java.util.function.Function;

public class MappingSubscriber<T> implements Flow.Subscriber<Integer> {
  private final Flow.Subscriber<? super T> downstream;
  private final Function<Integer, ? extends T> mapper;

  public MappingSubscriber(Flow.Subscriber<? super T> downstream, Function<Integer, ? extends T> mapper) {
    this.downstream = downstream;
    this.mapper = mapper;
  }

  @Override public void onSubscribe(Flow.Subscription s) { downstream.onSubscribe(s); }
  @Override public void onNext(Integer item) { downstream.onNext(mapper.apply(item)); }
  @Override public void onError(Throwable t) { downstream.onError(t); }
  @Override public void onComplete() { downstream.onComplete(); }
}