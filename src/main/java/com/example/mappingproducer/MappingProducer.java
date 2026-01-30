package com.example.mappingproducer;

import java.util.concurrent.Flow;
import java.util.function.Consumer;
import java.util.function.Function;

public class MappingProducer<T> implements Flow.Publisher<T> {
  private final Flow.Publisher<T> source;

  private MappingProducer(Flow.Publisher<T> source) {
    this.source = source;
  }

  // Static Factory
  public static <T> MappingProducer<T> from(Flow.Publisher<T> source) {
    return new MappingProducer<>(source);
  }

  // -----------------------------------------------------------
  // OPERATOR: MAP
  // Returns a NEW MappingProducer around a NEW Anonymous Publisher
  // -----------------------------------------------------------
  public <R> MappingProducer<R> map(Function<T, R> mapper) {
    // We create a new Publisher that sits between the source and the generic subscriber
    Flow.Publisher<R> mappedSource = downstream -> source.subscribe(new Flow.Subscriber<T>() {
      @Override
      public void onSubscribe(Flow.Subscription s) {
        downstream.onSubscribe(s);
      }

      @Override
      public void onNext(T item) {
        try {
          // Apply the transformation
          R result = mapper.apply(item);
          downstream.onNext(result);
        } catch (Throwable t) {
          downstream.onError(t);
          // Optional: Cancel upstream subscription if possible,
          // but strictly we just error out here.
        }
      }

      @Override
      public void onError(Throwable t) {
        downstream.onError(t);
      }

      @Override
      public void onComplete() {
        downstream.onComplete();
      }
    });

    return new MappingProducer<>(mappedSource);
  }

  // -----------------------------------------------------------
  // CONVENIENCE: SUBSCRIBE (Consumer)
  // -----------------------------------------------------------
  public void subscribe(Consumer<T> consumer) {
    this.subscribe(new Flow.Subscriber<T>() {
      @Override
      public void onSubscribe(Flow.Subscription s) {
        s.request(Long.MAX_VALUE);
      }
      @Override
      public void onNext(T item) {
        consumer.accept(item);
      }
      @Override
      public void onError(Throwable t) {
        t.printStackTrace();
      }
      @Override
      public void onComplete() {
        // No-op
      }
    });
  }

  // Standard Publisher implementation
  @Override
  public void subscribe(Flow.Subscriber<? super T> subscriber) {
    source.subscribe(subscriber);
  }
}