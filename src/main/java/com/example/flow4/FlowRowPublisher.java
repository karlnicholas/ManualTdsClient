package com.example.flow4;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;

public class FlowRowPublisher implements Flow.Publisher<FlowRow> {
  private final List<List<byte[]>> list;

  public FlowRowPublisher() {
    this.list = List.of(
        List.of(new byte[]{1}, new byte[]{2}, new byte[]{3}),
        List.of(new byte[]{2}, new byte[]{3}, new byte[]{4}),
        List.of(new byte[]{4}, new byte[]{5}, new byte[]{6})
    );
  }
  @Override
  public void subscribe(Flow.Subscriber<? super FlowRow> subscriber) {
    subscriber.onSubscribe(new Flow.Subscription() {
      @Override
      public void request(long n) {
        ExecutorService threadPoolExecutor = Executors.newSingleThreadExecutor();
        threadPoolExecutor.submit(() -> {
          for( List<byte[]> i : list) {
            List<String> columnNames = new ArrayList<>();
            for(int colIndex=0; colIndex<i.size(); colIndex++) {
              columnNames.add("COL"+colIndex);
            }
            FlowRowImpl flowRow = new FlowRowImpl(i, columnNames);
            subscriber.onNext(flowRow);
          }
          subscriber.onComplete();
        });
        threadPoolExecutor.shutdown();
      }

      @Override
      public void cancel() {

      }
    });
  }

}
