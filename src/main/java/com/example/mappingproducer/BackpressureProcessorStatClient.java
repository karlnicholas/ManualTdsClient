package com.example.mappingproducer;

import org.apache.commons.math3.stat.descriptive.StatisticalSummary;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class BackpressureProcessorStatClient {

  public static void main(String[] args) throws InterruptedException {
    new BackpressureProcessorStatClient().run();
  }

  private void run() throws InterruptedException {
    StatisticalProcessor statPub = new RandomPublisher(50).stats();
    CountDownLatch latch = new CountDownLatch(1);

    ClientSubscriber client = new ClientSubscriber(latch, "c1");
    statPub.subscribe(client);

    // 1. Request 2 summaries (Processor will ask for 20 doubles)
    client.pull(2);
    Thread.sleep(500); // Wait and observe output

    // 2. Request 1 more summary (Processor will ask for 10 doubles)
    client.pull(1);
    Thread.sleep(500);

    // 3. Request more than remains (Source has 50 total, we used 30, request 5 more)
    // This will trigger the onComplete and the "Flush" logic.
    client.pull(5);

    latch.await();
  }

  // =================================================================================
  // 1. THE GENERIC SOURCE
  // =================================================================================
  static class RandomPublisher implements Flow.Publisher<Double> {
    private final long MAX_LIMIT;

    RandomPublisher(long maxLimit) {
      MAX_LIMIT = maxLimit;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super Double> subscriber) {
      subscriber.onSubscribe(new RandomSubscription(subscriber, MAX_LIMIT));
    }

    public StatisticalProcessor stats() {
      return new StatisticalProcessor(this);
    }

  }
  // =================================================================================
  // Processor
  // =================================================================================
  static class StatisticalProcessor implements Flow.Processor<Double, StatisticalSummary> {
    private Flow.Subscriber<? super StatisticalSummary> downstream;
    private Flow.Subscription upstreamSubscription;
    private final Flow.Publisher<Double> upstream;

    private final int windowSize = 10; // How many doubles per summary
    private final SummaryStatistics stats = new SummaryStatistics();
    private int countInWindow = 0;

    StatisticalProcessor(Flow.Publisher<Double> upstream) {
      this.upstream = upstream;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super StatisticalSummary> subscriber) {
      this.downstream = subscriber;
      upstream.subscribe(this);
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      this.upstreamSubscription = subscription;

      // We pass a custom subscription to the downstream
      downstream.onSubscribe(new Flow.Subscription() {
        @Override
        public void request(long n) {
          if (n <= 0) return;
          // To provide N summaries, we need N * windowSize doubles from upstream
          upstreamSubscription.request(n * windowSize);
        }

        @Override
        public void cancel() {
          upstreamSubscription.cancel();
        }
      });
    }

    @Override
    public void onNext(Double item) {
      stats.addValue(item);
      countInWindow++;

      // Once we hit the window size, emit a summary
      if (countInWindow == windowSize) {
        downstream.onNext(stats.getSummary());
        stats.clear(); // Reset for the next summary
        countInWindow = 0;
      }
    }

    @Override
    public void onError(Throwable throwable) {
      downstream.onError(throwable);
    }

    @Override
    public void onComplete() {
      // If there are leftover values that didn't fill a window,
      // you could choose to emit one last partial summary here.
      downstream.onComplete();
    }
  }

    // =================================================================================
  // 4. THE SINK
  // =================================================================================
    static class ClientSubscriber implements Flow.Subscriber<StatisticalSummary> {
      private final CountDownLatch latch;
      private final String name;
      private Flow.Subscription subscription; // Store this!

      ClientSubscriber(CountDownLatch latch, String name) {
        this.latch = latch;
        this.name = name;
      }

      @Override
      public void onSubscribe(Flow.Subscription s) {
        this.subscription = s;
        // Don't request anything yet, or request just a starting amount
      }

      // New method to allow manual pulling of data
      public void pull(long n) {
        if (subscription != null) {
          System.out.println(name + " requesting " + n + " more summaries...");
          subscription.request(n);
        }
      }

      @Override
      public void onNext(StatisticalSummary item) {
        System.out.println(name + " RECEIVED: " + item.getMean());
      }

      @Override
      public void onError(Throwable t) { t.printStackTrace(); latch.countDown(); }

      @Override
      public void onComplete() {
        System.out.println(name + ": onComplete called");
        latch.countDown();
      }
    }

  // =================================================================================
  // THE CORRECTED WORKER LOGIC
  // =================================================================================
  static class RandomSubscription implements Flow.Subscription {
    private final Flow.Subscriber <? super Double> subscriber;
    private final AtomicLong demand = new AtomicLong();
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private volatile boolean cancelled = false;
    private final long MAX_LIMIT;
    private long totalProduced = 0;

    RandomSubscription(Flow.Subscriber<? super Double> subscriber, Long MAX_LIMIT) {
      this.subscriber = subscriber;
      this.MAX_LIMIT = MAX_LIMIT;

    }

    @Override
    public void request(long n) {
      if (n <= 0) return;
      demand.addAndGet(n); // Track how many items are requested total
      // Use compareAndSeInteger to ensure only one thread is pushing data aInteger a time
      if (isRunning.compareAndSet(false, true)) {
        ForkJoinPool.commonPool().execute(this::drain);
      }
    }

    private void drain() {
      try {
        while (demand.get() > 0 && totalProduced < MAX_LIMIT && !cancelled) {
          subscriber.onNext(ThreadLocalRandom.current().nextDouble());
          demand.decrementAndGet();
          totalProduced++;
        }

        if (totalProduced >= MAX_LIMIT && !cancelled) {
          subscriber.onComplete();
        }
      } catch (Exception e) {
        subscriber.onError(e);
      } finally {
        isRunning.set(false);
        // If new demand arrived while we were processing, kick off a new task
        if (demand.get() > 0 && totalProduced < MAX_LIMIT && !cancelled) {
          if (isRunning.compareAndSet(false, true)) {
            ForkJoinPool.commonPool().execute(this::drain);
          }
        }
      }
    }
    @Override
    public void cancel() { cancelled = true; }
  }
}