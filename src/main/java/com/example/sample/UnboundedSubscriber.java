package com.example.sample;

import java.util.concurrent.Flow;

class UnboundedSubscriber<T> implements Flow.Subscriber<T> {
  public void onSubscribe(Flow.Subscription subscription) {
    subscription.request(Long.MAX_VALUE); // effectively unbounded
  }
  public void onNext(T item) { use(item); }
  public void onError(Throwable ex) { ex.printStackTrace(); }
  public void onComplete() {}
  void use(T item) {  }
}
