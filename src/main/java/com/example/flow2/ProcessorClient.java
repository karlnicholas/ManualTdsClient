package com.example.flow2;

import java.util.concurrent.CountDownLatch;

public class ProcessorClient {
  public static void main(String[] args) throws InterruptedException {
    new ProcessorClient().run4();
  }

  private void run4() throws InterruptedException {
    // instance Statement
    FlowStatement statement = new FlowStatementImpl();

    CountDownLatch latch = new CountDownLatch(1);

    // This matches your desired "wrapped" look
    FlowResultWrapper<String> wrappedFlow = FlowResultWrapper.wrap(statement.execute())
            .map(i -> "Number: " + i);

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
