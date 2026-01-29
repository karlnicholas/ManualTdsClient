package com.example.flow3;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;

public class ProcessorClient {
  public static void main(String[] args) throws InterruptedException {
    new ProcessorClient().run3();
  }

  private void run3() throws InterruptedException {
    // instance Integer publisher
    FlowRowPublisher publisher = new FlowRowPublisher();

    // instance mapping publisher
    FlowResult flowResult = new FlowResultImpl(publisher);
    // set the mapping function, instance and retrieve the mapping publisher
//    Flow.Publisher<Integer> resultPublisher = flowResult.map(i -> i*5);
    Flow.Publisher<OutRecord> resultPublisher = flowResult.map((row, rowMetadata) -> {
          OutRecord outRecord= new OutRecord(
              row.get(0, String.class),
              row.get(1, String.class),
              row.get(2, String.class)
          );
          return outRecord;
      }
    );

    CountDownLatch latch = new CountDownLatch(1);

    // subscribe the mapping publisher
    resultPublisher.subscribe(new SystemOutSubscriber(null, ()->latch.countDown()));

    latch.await();
  }

}
