package com.example.flow2;

import java.util.concurrent.Flow;
import java.util.function.Function;

public class IncrementProcessor implements Flow.Processor<Integer, Integer>{
  private final Flow.Publisher<Integer> upstream;
  private final Function<Integer, Integer> mapper;
  public IncrementProcessor(Flow.Publisher<Integer> upstream,  Function<Integer, Integer> mapper) {
    this.upstream = upstream;
    this.mapper = mapper;
  }
  private Flow.Subscriber<? super Integer> subscriber;
  private Flow.Subscription subscription;

  @Override
  public void subscribe(Flow.Subscriber<? super Integer> subscriber) {
    this.subscriber = subscriber;
    upstream.subscribe(this);
  }

  @Override
  public void onSubscribe(Flow.Subscription subscription) {
    subscriber.onSubscribe(subscription);
  }

  @Override
  public void onNext(Integer item) {
    subscriber.onNext(mapper.apply(item));
  }

  @Override
  public void onError(Throwable throwable) {
    subscriber.onError(throwable);
  }

  @Override
  public void onComplete() {
    subscriber.onComplete();
  }
}
