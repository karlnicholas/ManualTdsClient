package com.example.flow4;

import java.util.concurrent.Flow;
import java.util.function.Consumer;

public class SystemOutSubscriber implements Flow.Subscriber<OutRecord>{
  private final Consumer<Throwable> onErrorConsumer;
  private final Runnable onCompleteConsumer;
  public SystemOutSubscriber(Consumer<Throwable> onErrorConsumer, Runnable onCompleteConsumer) {
    this.onErrorConsumer = onErrorConsumer;
    this.onCompleteConsumer = onCompleteConsumer;
  }
  @Override
  public void onSubscribe(Flow.Subscription subscription) {
    subscription.request(Long.MAX_VALUE);
  }

  @Override
  public void onNext(OutRecord outRecord) {
    System.out.println(outRecord);
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
