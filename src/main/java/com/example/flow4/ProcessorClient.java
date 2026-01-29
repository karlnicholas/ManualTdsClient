package com.example.flow4;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;

public class ProcessorClient {
  public static void main(String[] args) throws InterruptedException {
    new ProcessorClient().run3();
  }

  private void run3() throws InterruptedException {
    // instance Statement
    FlowStatement statement = new FlowStatementImpl(new FlowRowPublisher());

    CountDownLatch latch = new CountDownLatch(1);

    // This matches your desired "wrapped" look
    FlowResultWrapper<OutRecord> wrappedFlow = FlowResultWrapper.wrap(statement.execute())
        .map((row, rowMetadata) -> new OutRecord(
              row.get(0, String.class),
              row.get(1, String.class),
              row.get(2, String.class))
        );

    // Subscribing is now a one-liner
    SimpleSubscriber.subscribe(wrappedFlow,
        System.out::println,
        ()->latch.countDown(),
        throwable ->  {
          System.out.println("Error: " + throwable.getMessage());
          latch.countDown();}
    );

    latch.await();
  }

}
