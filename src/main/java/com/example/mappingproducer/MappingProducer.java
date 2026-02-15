package com.example.mappingproducer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

public class MappingProducer<T> implements Flow.Publisher<T> {

  public static void main(String[] args) {
    System.out.println("Main Thread Start: " + Thread.currentThread().getName());

    MappingProducer.just(1)
        .map(i -> i + 1)
        .subscribe(i -> System.out.println("Value: " + i));

    System.out.println("Main Thread End");

  }

  private final Flow.Publisher<T> source;
  private final String name;
  private static final AtomicInteger ID_COUNTER = new AtomicInteger(1);

  private MappingProducer(Flow.Publisher<T> source, String name) {
    this.source = source;
    this.name = name;
  }

  private static void log(String prefix, String message) {
    System.out.printf("[%s] %-20s : %s%n", Thread.currentThread().getName(), prefix, message);
  }

  @Override
  public void subscribe(Flow.Subscriber<? super T> subscriber) {
    source.subscribe(subscriber);
  }

  // =================================================================================
  // 1. JUST (The Source)
  // =================================================================================
  public static <T> MappingProducer<T> just(T value) {
    String opName = "Source-Just(" + value + ")";
    log("Assembly", "Creating " + opName);

    // No anonymous class here. We use the explicit 'RandomPublisher'.
    JustPublisher<T> publisher = new JustPublisher<>(value, opName);

    return new MappingProducer<>(publisher, opName);
  }

  // --- Explicit Classes for 'Just' ---

  // The Publisher
  private static class JustPublisher<T> implements Flow.Publisher<T> {
    private final T value;
    private final String opName;

    public JustPublisher(T value, String opName) {
      this.value = value;
      this.opName = opName;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super T> subscriber) {
      log("Subscribe", opName + " received subscription request");
      // Pass dependencies manually to the RandomSubscription
      subscriber.onSubscribe(new JustSubscription<>(subscriber, value, opName));
    }
  }

  // The RandomSubscription
  private static class JustSubscription<T> implements Flow.Subscription {
    private final Flow.Subscriber<? super T> subscriber;
    private final T value;
    private final String opName;
    private final AtomicBoolean executed = new AtomicBoolean(false);
    private volatile boolean cancelled = false;

    public JustSubscription(Flow.Subscriber<? super T> subscriber, T value, String opName) {
      this.subscriber = subscriber;
      this.value = value;
      this.opName = opName;
    }

    @Override
    public void request(long n) {
      log("Request", opName + " request(" + n + ")");
      if (n > 0 && !cancelled && executed.compareAndSet(false, true)) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        log("Execution", opName + " submitting task to Executor");

        // Pass dependencies manually to the Task
        executor.submit(new JustEmissionTask<>(subscriber, value, opName, executor, this));
      }
    }

    @Override
    public void cancel() {
      log("Cancel", opName + " cancelled");
      cancelled = true;
    }

    public boolean isCancelled() { return cancelled; }
  }

  // The Runnable Task
  private static class JustEmissionTask<T> implements Runnable {
    private final Flow.Subscriber<? super T> subscriber;
    private final T value;
    private final String opName;
    private final ExecutorService executor;
    private final JustSubscription<T> parent;

    public JustEmissionTask(Flow.Subscriber<? super T> sub, T val, String name, ExecutorService exec, JustSubscription<T> parent) {
      this.subscriber = sub;
      this.value = val;
      this.opName = name;
      this.executor = exec;
      this.parent = parent;
    }

    @Override
    public void run() {
      log("Async-Task", opName + " starting emission");
      if (!parent.isCancelled()) {
        try {
          log("Async-Task", opName + " pushing onNext(" + value + ")");
          subscriber.onNext(value);
          if (!parent.isCancelled()) {
            log("Async-Task", opName + " pushing onComplete()");
            subscriber.onComplete();
          }
        } catch (Throwable t) {
          subscriber.onError(t);
        }
      }
      executor.shutdown();
    }
  }

  // =================================================================================
  // 2. MAP (The Operator)
  // =================================================================================
  public <R> MappingProducer<R> map(Function<T, R> mapper) {
    String opName = "MapOp-" + ID_COUNTER.getAndIncrement();
    log("Assembly", "Creating " + opName + " wrapping " + this.name);

    // We explicitly pass 'this' (the upstream source) into the new Publisher
    MapPublisher<T, R> mappedSource = new MapPublisher<>(this, mapper, opName);

    return new MappingProducer<>(mappedSource, opName);
  }

  // --- Explicit Classes for 'Map' ---

  // The Publisher (The Decorator)
  private static class MapPublisher<T, R> implements Flow.Publisher<R> {
    private final Flow.Publisher<T> upstream; // The "Captured" Source
    private final Function<T, R> mapper;      // The "Captured" Function
    private final String opName;

    public MapPublisher(Flow.Publisher<T> upstream, Function<T, R> mapper, String opName) {
      this.upstream = upstream;
      this.mapper = mapper;
      this.opName = opName;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super R> downstream) {
      log("Subscribe", opName + " received subscription request");

      // We create the Middleman (ClientSubscriber) and connect it to Upstream
      upstream.subscribe(new MapSubscriber<>(downstream, mapper, opName));
    }
  }

  // The ClientSubscriber (The Middleman)
  private static class MapSubscriber<T, R> implements Flow.Subscriber<T> {
    private final Flow.Subscriber<? super R> downstream; // The "Captured" Downstream
    private final Function<T, R> mapper;
    private final String opName;

    public MapSubscriber(Flow.Subscriber<? super R> downstream, Function<T, R> mapper, String opName) {
      this.downstream = downstream;
      this.mapper = mapper;
      this.opName = opName;
    }

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
  }

  // =================================================================================
  // 3. SUBSCRIBE (The Convenience Method)
  // =================================================================================
  public void subscribe(Consumer<T> consumer) {
    log("Subscribe", "User called subscribe()");
    this.subscribe(new EndSubscriber<>(consumer));
  }

  // The Final ClientSubscriber
  private static class EndSubscriber<T> implements Flow.Subscriber<T> {
    private final Consumer<T> consumer;
    private final String subName = "Final-ClientSubscriber";

    public EndSubscriber(Consumer<T> consumer) {
      this.consumer = consumer;
    }

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
  }
}