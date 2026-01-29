package com.example.flow4;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;

public class ProcessorClient {
  public static void main(String[] args) throws InterruptedException {
    new ProcessorClient().run3();
  }

  private void run3() throws InterruptedException {
    FlowRowPublisher source = new FlowRowPublisher();
    FlowStatement statement = new FlowStatementImpl(source);
    CountDownLatch latch = new CountDownLatch(1);

    // 1. CAPTURE the Execution (Publisher<FlowResult>)
    // Equivalent to: Mono.from(connection.createStatement(...).execute())
    Flow.Publisher<? extends FlowResult> execution = statement.execute();

    // 2. MAP the result to your DTOs
    // Equivalent to: rv.flatMapMany(result -> result.map(mapper))
    FlowResultWrapper<OutRecord> wrappedFlow = FlowResultWrapper.mapFrom(execution,
        (row, rowMetadata) -> new OutRecord(
            row.get(0, String.class),
            row.get(1, String.class),
            row.get(2, String.class))
    );

    // 3. Subscribe
    SimpleSubscriber.subscribe(wrappedFlow,
        System.out::println,
        latch::countDown,
        throwable -> {
          System.out.println("Error: " + throwable.getMessage());
          source.cancel();
          latch.countDown();
        }
    );

    latch.await();
  }
}