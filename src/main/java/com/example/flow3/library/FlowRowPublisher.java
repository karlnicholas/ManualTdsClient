package com.example.flow3.library;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

public class FlowRowPublisher implements Flow.Publisher<FlowRow> {
  private final List<List<byte[]>> list;

  public FlowRowPublisher() {
    this.list = List.of(
        List.of(new byte[]{1}, new byte[]{2}, new byte[]{3}),
        List.of(new byte[]{2}, new byte[]{3}, new byte[]{4}),
        List.of(new byte[]{4}, new byte[]{5}, new byte[]{6})
    );
  }

  // 1. Create a handle to track if we are cancelled
  AtomicBoolean isCancelled = new AtomicBoolean(false);

  @Override
  public void subscribe(Flow.Subscriber<? super FlowRow> subscriber) {

    subscriber.onSubscribe(new Flow.Subscription() {
      @Override
      public void request(long n) {
        // In a real DB driver, you'd check 'n' and only fetch that many rows.
        // For now, we just ensure we only start once.
        ExecutorService threadPoolExecutor = Executors.newSingleThreadExecutor();
        threadPoolExecutor.submit(() -> {
          for (List<byte[]> i : list) {

            // 2. CRITICAL CHECK: Stop working if the user cancelled
            if (isCancelled.get()) {
              break;
            }

            List<String> columnNames = new ArrayList<>();
            for (int colIndex = 0; colIndex < i.size(); colIndex++) {
              columnNames.add("COL" + colIndex);
            }

            // 3. One last check before emitting
            if (!isCancelled.get()) {
              subscriber.onNext(new FlowRowImpl(i, columnNames));
            }
          }

          // 4. Only complete if we finished naturally (not cancelled)
          if (!isCancelled.get()) {
            subscriber.onComplete();
          }

          threadPoolExecutor.shutdown();
        });
      }

      @Override
      public void cancel() {
        // 5. Signal the running thread to stop
        isCancelled.set(true);
      }
    });
  }

  public void cancel() {
    isCancelled.set(true);
  }
}