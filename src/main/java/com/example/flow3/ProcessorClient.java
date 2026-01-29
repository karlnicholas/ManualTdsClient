package com.example.flow3;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.function.Function;

public class ProcessorClient {
  public static void main(String[] args) throws InterruptedException {
    new ProcessorClient().run4();
  }

  private void run4() throws InterruptedException {
    // instance Integer publisher
    FlowStatement statement = new IntegerFlowStatement();

    // This matches your desired "wrapped" look
    FlowResultWrapper<String> wrappedFlow = FlowResultWrapper.wrap(statement.execute())
            .map(i -> "Number: " + i);

    CountDownLatch latch = new CountDownLatch(1);

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
