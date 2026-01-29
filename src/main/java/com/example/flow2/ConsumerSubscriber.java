package com.example.flow2;

import java.util.concurrent.Flow;
import java.util.function.Consumer;

public class ConsumerSubscriber<T> implements Flow.Subscriber<T> {
  private final Consumer<T> onNextConsumer;
  private final Consumer<Throwable> onErrorConsumer;
  private final Runnable onCompleteRunnable;
  public ConsumerSubscriber(Consumer<T> onNextConsumer, Consumer<Throwable> onErrorConsumer, Runnable onCompleteRunnable) {
    this.onNextConsumer = onNextConsumer;
    this.onErrorConsumer = onErrorConsumer;
    this.onCompleteRunnable = onCompleteRunnable;
  }
  @Override
  public void onSubscribe(Flow.Subscription subscription) {
    subscription.request(Long.MAX_VALUE);
  }

  @Override
  public void onNext(T item) {
    onNextConsumer.accept(item);
  }

  @Override
  public void onError(Throwable throwable) {
    onErrorConsumer.accept(throwable);
  }

  @Override
  public void onComplete() {
    onCompleteRunnable.run();
  }
}
