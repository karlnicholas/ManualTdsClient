package com.example.flow;

import java.util.concurrent.Flow;
import java.util.function.Function;

public class MappingSubscriber<T, R> implements Flow.Subscriber<T> {
  private final Flow.Subscriber<? super R> downstream;
  private final Function<? super T, ? extends R> mapper;

  public MappingSubscriber(Flow.Subscriber<? super R> downstream,
                           Function<? super T, ? extends R> mapper) {
    this.downstream = downstream;
    this.mapper = mapper;
  }

  @Override
  public void onSubscribe(Flow.Subscription subscription) {
    // Pass the subscription straight through so the end-user can request data
    downstream.onSubscribe(subscription);
  }

  @Override
  public void onNext(T item) {
    try {
      // The "Transformation" step
      R mappedItem = mapper.apply(item);
      downstream.onNext(mappedItem);
    } catch (Throwable t) {
      // If mapping fails, shut down the stream and report the error
      onError(t);
    }
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