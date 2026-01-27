package com.example.flow;

import java.util.concurrent.Flow;
import java.util.function.Consumer;

public class SystemOutSubscriber implements Flow.Subscriber<Integer>{
  private final Consumer<Throwable> onErrorConsumer;
  private final Runnable onCompleteConsumer;
  public SystemOutSubscriber(Consumer<Throwable> onErrorConsumer, Runnable onCompleteConsumer) {
    this.onErrorConsumer = onErrorConsumer;
    this.onCompleteConsumer = onCompleteConsumer;
  }
  @Override
  public void onSubscribe(Flow.Subscription subscription) {
    System.out.println("Subscribed. Requesting Long.MAX_VALUE items.");
    subscription.request(Long.MAX_VALUE);
  }

  @Override
  public void onNext(Integer item) {
    System.out.println("Item: " + item);
  }

  @Override
  public void onError(Throwable throwable) {
    if ( onErrorConsumer != null ) {
      onErrorConsumer.accept(throwable);
    }
    System.out.println("Error: " + throwable.getMessage());
  }

  @Override
  public void onComplete() {
    if ( onCompleteConsumer != null ) {
      onCompleteConsumer.run();
    }
    System.out.println("Done.");
  }
}
