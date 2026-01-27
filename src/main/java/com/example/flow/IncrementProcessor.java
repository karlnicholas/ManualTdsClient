package com.example.flow;

import java.util.concurrent.Flow;
import java.util.function.Function;

public class IncrementProcessor implements Flow.Processor<Integer, Integer>{
  private final Flow.Publisher<Integer> upstream;
  public IncrementProcessor(Flow.Publisher<Integer> upstream) {
    this.upstream = upstream;
  }
  private Flow.Subscriber<? super Integer> subscriber;
  private Flow.Subscription subscription;
  Function<Integer, Integer> mapper = i -> i + 1;

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
