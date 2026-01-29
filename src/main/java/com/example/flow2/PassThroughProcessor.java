package com.example.flow2;

import java.util.concurrent.Flow;

public class PassThroughProcessor implements Flow.Processor<Integer, FlowResult>{
  private final FlowResult flowResult;
  private final Flow.Publisher<Integer> upstream;
  public PassThroughProcessor(Flow.Publisher<Integer> upstream) {
    this.upstream = upstream;
    flowResult = new FlowResultImplOrig(upstream);
  }
  private Flow.Subscriber<? super FlowResult> subscriber;
  private Flow.Subscription subscription;

  @Override
  public void subscribe(Flow.Subscriber<? super FlowResult> subscriber) {
    this.subscriber = subscriber;
    upstream.subscribe(this);
  }

  @Override
  public void onSubscribe(Flow.Subscription subscription) {
    subscriber.onSubscribe(subscription);
  }

  @Override
  public void onNext(Integer item) {
    subscriber.onNext(flowResult);
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
