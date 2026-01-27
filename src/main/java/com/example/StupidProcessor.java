package com.example;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class StupidProcessor<I, O> implements Publisher<O>, Subscriber<I> {

  private final java.util.function.Function<I, O> mapper;
  private Subscriber<? super O> downstream;

  public StupidProcessor(java.util.function.Function<I, O> mapper) {
    this.mapper = mapper;
  }

  // --- Publisher Logic (The "Front" facing you) ---
  @Override
  public void subscribe(Subscriber<? super O> s) {
    this.downstream = s;
    // In a real app, we'd wait for the upstream subscription.
    // Here, we just give the downstream a 'fake' subscription
    // that does nothing when they ask for data.
    s.onSubscribe(new Subscription() {
      @Override public void request(long n) { /* I'm ignoring you! */ }
      @Override public void cancel() { /* Too bad! */ }
    });
  }

  // --- Subscriber Logic (The "Back" facing the Data Source) ---
  @Override
  public void onSubscribe(Subscription s) {
    // Immediately demand everything from the source
    s.request(Long.MAX_VALUE);
  }

  @Override
  public void onNext(I item) {
    // The "Map" part: Transform and immediately shove downstream
    O mapped = mapper.apply(item);
    downstream.onNext(mapped);
  }

  @Override
  public void onError(Throwable t) {
    downstream.onError(t);
  }

  @Override
  public void onComplete() {
    downstream.onComplete();
  }
}