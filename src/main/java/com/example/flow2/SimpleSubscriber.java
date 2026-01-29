package com.example.flow2;

import java.util.concurrent.Flow;
import java.util.function.Consumer;

public class SimpleSubscriber<T> implements Flow.Subscriber<T> {
  private final Consumer<T> nextHandler;
  private final Runnable onCompleteHandler;
  private final Consumer<Throwable> onErrorHandler;

  public SimpleSubscriber(Consumer<T> nextHandler, Runnable onCompleteHandler,  Consumer<Throwable> onErrorHandler) {
    this.nextHandler = nextHandler;
    this.onCompleteHandler = onCompleteHandler;
    this.onErrorHandler = onErrorHandler;
  }

  @Override
  public void onSubscribe(Flow.Subscription s) {
    s.request(Long.MAX_VALUE); // Auto-request everything
  }

  @Override
  public void onNext(T item) {
    nextHandler.accept(item);
  }

  @Override public void onError(Throwable t) { t.printStackTrace(); }
  @Override public void onComplete() {
    onCompleteHandler.run();
  }

  // Helper method to make the client call even cleaner
  public static <T> void subscribe(Flow.Publisher<T> publisher, Consumer<T> handler, Runnable onCompleteHandler,  Consumer<Throwable> onErrorHandler) {
    publisher.subscribe(new SimpleSubscriber<>(handler, onCompleteHandler,  onErrorHandler));
  }
}