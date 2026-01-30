package com.example.mappingproducer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

public class MappingProducer<T> implements Flow.Publisher<T> {
  private final Flow.Publisher<T> source;

  private MappingProducer(Flow.Publisher<T> source) {
    this.source = source;
  }

  // -----------------------------------------------------------
  // 1. FACTORY: FROM (Direct Copy - No Changes)
  // -----------------------------------------------------------
  public static <T> MappingProducer<T> from(Flow.Publisher<T> source) {
    return new MappingProducer<>(source);
  }

  // -----------------------------------------------------------
  // 2. FACTORY: JUST (Async / Threaded - Direct Copy)
  // -----------------------------------------------------------
  public static <T> MappingProducer<T> just(T value) {
    return new MappingProducer<>(subscriber -> {
      subscriber.onSubscribe(new Flow.Subscription() {
        private final AtomicBoolean executed = new AtomicBoolean(false);
        private volatile boolean cancelled = false;

        @Override
        public void request(long n) {
          if (n > 0 && !cancelled && executed.compareAndSet(false, true)) {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.submit(() -> {
              if (!cancelled) {
                try {
                  subscriber.onNext(value);
                  if (!cancelled) subscriber.onComplete();
                } catch (Throwable t) {
                  subscriber.onError(t);
                }
              }
              executor.shutdown();
            });
          }
        }

        @Override public void cancel() { cancelled = true; }
      });
    });
  }

  // -----------------------------------------------------------
  // 3. OPERATOR: MAP (Direct Copy - No Changes)
  // Since map is linear, simple delegation is 100% correct here.
  // -----------------------------------------------------------
  public <R> MappingProducer<R> map(Function<T, R> mapper) {
    Flow.Publisher<R> mappedSource = downstream -> source.subscribe(new Flow.Subscriber<T>() {
      @Override
      public void onSubscribe(Flow.Subscription s) {
        downstream.onSubscribe(s);
      }

      @Override
      public void onNext(T item) {
        try {
          R result = mapper.apply(item);
          downstream.onNext(result);
        } catch (Throwable t) {
          downstream.onError(t);
        }
      }

      @Override public void onError(Throwable t) { downstream.onError(t); }
      @Override public void onComplete() { downstream.onComplete(); }
    });

    return new MappingProducer<>(mappedSource);
  }

  // -----------------------------------------------------------
  // 4. OPERATOR: FLATMAP (The "Arbiter" Version)
  // This replaces your previous simple implementation.
  // -----------------------------------------------------------
  public <R> MappingProducer<R> flatMap(Function<T, Flow.Publisher<R>> mapper) {
    return new MappingProducer<>(downstream -> {
      // Use the Arbiter to safely coordinate the main source and the inner source
      FlatMapSubscriber<T, R> arbiter = new FlatMapSubscriber<>(downstream, mapper);
      source.subscribe(arbiter);
    });
  }

  // Overload: Success Consumer + Error Consumer
  public void subscribe(Consumer<T> onNext, Consumer<Throwable> onError) {
    this.subscribe(onNext, onError, () -> {});
  }

  // Full Overload: Success + Error + Completion
  public void subscribe(Consumer<T> onNext, Consumer<Throwable> onError, Runnable onComplete) {
    this.subscribe(new Flow.Subscriber<T>() {
      @Override
      public void onSubscribe(Flow.Subscription s) {
        s.request(Long.MAX_VALUE);
      }

      @Override
      public void onNext(T item) {
        onNext.accept(item);
      }

      @Override
      public void onError(Throwable t) {
        onError.accept(t);
      }

      @Override
      public void onComplete() {
        onComplete.run();
      }
    });
  }

  // -----------------------------------------------------------
  // INNER CLASS: THE ARBITER (Required for flatMap)
  // -----------------------------------------------------------
  private static class FlatMapSubscriber<T, R> implements Flow.Subscriber<T>, Flow.Subscription {
    private final Flow.Subscriber<? super R> downstream;
    private final Function<T, Flow.Publisher<R>> mapper;

    private Flow.Subscription mainSubscription;
    private final AtomicReference<Flow.Subscription> innerSubscription = new AtomicReference<>();
    private final AtomicLong requested = new AtomicLong(0);
    private final AtomicBoolean isCancelled = new AtomicBoolean(false);

    FlatMapSubscriber(Flow.Subscriber<? super R> downstream, Function<T, Flow.Publisher<R>> mapper) {
      this.downstream = downstream;
      this.mapper = mapper;
    }

    // --- Downstream calls this (The Subscription we give them) ---
    @Override
    public void request(long n) {
      if (n <= 0) return;
      requested.addAndGet(n);

      Flow.Subscription inner = innerSubscription.get();
      if (inner != null) {
        inner.request(n);
      } else if (mainSubscription != null) {
        // If no inner yet, ask main source for the item to start the flatMap process
        mainSubscription.request(1);
      }
    }

    @Override
    public void cancel() {
      isCancelled.set(true);
      if (mainSubscription != null) mainSubscription.cancel();
      Flow.Subscription inner = innerSubscription.get();
      if (inner != null) inner.cancel();
    }

    // --- Main Source calls this ---
    @Override
    public void onSubscribe(Flow.Subscription s) {
      this.mainSubscription = s;
      downstream.onSubscribe(this); // Handshake with downstream
    }

    @Override
    public void onNext(T item) {
      if (isCancelled.get()) return;
      try {
        Flow.Publisher<R> innerPub = mapper.apply(item);

        // Subscribe to the inner publisher
        innerPub.subscribe(new Flow.Subscriber<R>() {
          @Override
          public void onSubscribe(Flow.Subscription s) {
            if (innerSubscription.compareAndSet(null, s)) {
              long r = requested.get();
              if (r > 0) s.request(r);
            }
          }
          @Override public void onNext(R item) { downstream.onNext(item); }
          @Override public void onError(Throwable t) { downstream.onError(t); }
          @Override public void onComplete() { downstream.onComplete(); }
        });
      } catch (Throwable t) {
        cancel();
        onError(t);
      }
    }

    @Override public void onError(Throwable t) { downstream.onError(t); }
    @Override public void onComplete() { /* Wait for inner to complete */ }
  }

  // -----------------------------------------------------------
  // CONVENIENCE: SUBSCRIBE
  // -----------------------------------------------------------
  public void subscribe(Consumer<T> consumer) {
    this.subscribe(new Flow.Subscriber<T>() {
      @Override public void onSubscribe(Flow.Subscription s) { s.request(Long.MAX_VALUE); }
      @Override public void onNext(T item) { consumer.accept(item); }
      @Override public void onError(Throwable t) { t.printStackTrace(); }
      @Override public void onComplete() { }
    });
  }

  @Override
  public void subscribe(Flow.Subscriber<? super T> subscriber) {
    source.subscribe(subscriber);
  }
}