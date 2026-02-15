package com.example.mappingproducer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

public class MappingProducerNoLambda<T> implements Flow.Publisher<T> {
  private final Flow.Publisher<T> source;
  private final String name;

  private static final AtomicInteger ID_COUNTER = new AtomicInteger(1);

  private MappingProducerNoLambda(Flow.Publisher<T> source, String name) {
    this.source = source;
    this.name = name;
  }

  private static void log(String prefix, String message) {
    System.out.printf("[%s] %-20s : %s%n", Thread.currentThread().getName(), prefix, message);
  }

  // -----------------------------------------------------------
  // FACTORY: JUST (Expanded)
  // -----------------------------------------------------------
  public static <T> MappingProducerNoLambda<T> just(final T value) {
    String opName = "Source-Just(" + value + ")";
    log("Assembly", "Creating " + opName);

    // EXPANDED: Explicit Publisher implementation instead of lambda
    Flow.Publisher<T> publisher = new Flow.Publisher<T>() {
      @Override
      public void subscribe(Flow.Subscriber<? super T> subscriber) {
        log("Subscribe", opName + " received subscription request");

        // EXPANDED: Explicit RepeatSubscription implementation
        subscriber.onSubscribe(new Flow.Subscription() {
          private final AtomicBoolean executed = new AtomicBoolean(false);
          private volatile boolean cancelled = false;

          @Override
          public void request(long n) {
            log("Request", opName + " request(" + n + ")");
            if (n > 0 && !cancelled && executed.compareAndSet(false, true)) {

              ExecutorService executor = Executors.newSingleThreadExecutor();
              log("Execution", opName + " submitting task to Executor");

              // EXPANDED: Explicit Runnable implementation
              executor.submit(new Runnable() {
                @Override
                public void run() {
                  log("Async-Task", opName + " starting emission");
                  if (!cancelled) {
                    try {
                      log("Async-Task", opName + " pushing onNext(" + value + ")");
                      subscriber.onNext(value);
                      if (!cancelled) {
                        log("Async-Task", opName + " pushing onComplete()");
                        subscriber.onComplete();
                      }
                    } catch (Throwable t) {
                      subscriber.onError(t);
                    }
                  }
                  executor.shutdown();
                }
              });
            }
          }

          @Override
          public void cancel() {
            log("Cancel", opName + " cancelled");
            cancelled = true;
          }
        });
      }
    };

    return new MappingProducerNoLambda<>(publisher, opName);
  }

  public static <T> MappingProducerNoLambda<T> from(Flow.Publisher<T> source) {
    return new MappingProducerNoLambda<>(source, "Source-From");
  }

  // -----------------------------------------------------------
  // OPERATOR: MAP (Expanded)
  // -----------------------------------------------------------
  public <R> MappingProducerNoLambda<R> map(final Function<T, R> mapper) {
    String opName = "MapOp-" + ID_COUNTER.getAndIncrement();
    log("Assembly", "Creating " + opName + " wrapping " + this.name);

    // EXPANDED: Explicit Publisher implementation
    Flow.Publisher<R> mappedSource = new Flow.Publisher<R>() {
      @Override
      public void subscribe(Flow.Subscriber<? super R> downstream) {
        log("Subscribe", opName + " received subscription request");

        // Subscribe to the UPSTREAM source (MappingProducer.this)
        MappingProducerNoLambda.this.subscribe(new Flow.Subscriber<T>() {

          @Override
          public void onSubscribe(Flow.Subscription s) {
            log("onSubscribe", opName + " passing subscription downstream");
            downstream.onSubscribe(s);
          }

          @Override
          public void onNext(T item) {
            try {
              log("onNext", opName + " received: " + item);
              R result = mapper.apply(item);
              log("onNext", opName + " transformed " + item + " -> " + result);
              downstream.onNext(result);
            } catch (Throwable t) {
              log("onError", opName + " transformation failed");
              downstream.onError(t);
            }
          }

          @Override
          public void onError(Throwable t) {
            log("onError", opName + " passing error downstream");
            downstream.onError(t);
          }

          @Override
          public void onComplete() {
            log("onComplete", opName + " passing complete downstream");
            downstream.onComplete();
          }
        });
      }
    };

    return new MappingProducerNoLambda<>(mappedSource, opName);
  }

  // -----------------------------------------------------------
  // CONVENIENCE: SUBSCRIBE (Expanded)
  // -----------------------------------------------------------
  public void subscribe(final Consumer<T> consumer) {
    String subName = "Final-ClientSubscriber";
    log("Subscribe", "User called subscribe()");

    this.subscribe(new Flow.Subscriber<T>() {
      @Override
      public void onSubscribe(Flow.Subscription s) {
        log("onSubscribe", subName + " received subscription. Requesting MAX.");
        s.request(Long.MAX_VALUE);
      }

      @Override
      public void onNext(T item) {
        log("onNext", subName + " received value: " + item);
        consumer.accept(item);
      }

      @Override
      public void onError(Throwable t) {
        log("onError", subName + " received error");
        t.printStackTrace();
      }

      @Override
      public void onComplete() {
        log("onComplete", subName + " completed");
      }
    });
  }

  @Override
  public void subscribe(Flow.Subscriber<? super T> subscriber) {
    source.subscribe(subscriber);
  }
}