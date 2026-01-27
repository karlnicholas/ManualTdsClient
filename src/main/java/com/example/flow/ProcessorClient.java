package com.example.flow;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.function.Function;

public class ProcessorClient {
  public static void main(String[] args) throws InterruptedException {
    new ProcessorClient().run3();
  }

  private void run3() throws InterruptedException {
    // instance Integer publisher
    IntegerPublisher publisher = new IntegerPublisher();
    // instance mapping publisher
    FlowResult flowResult = new FlowResultImpl(publisher);
    // set the mapping function, instance and retrieve the mapping publisher
    Flow.Publisher<Integer> resultPublisher = flowResult.map(i -> i+1);

    CountDownLatch latch = new CountDownLatch(1);
    Thread.sleep(500);

    // subscribe the mapping publisher
    resultPublisher.subscribe(new SystemOutSubscriber(null, ()->latch.countDown()));

    latch.await();
  }

  private Function<Integer, Integer> incrementMapper = i->i+1;

  private void run2() throws InterruptedException {
    IntegerPublisher publisher = new IntegerPublisher();
    IncrementProcessor processor1 = new IncrementProcessor(publisher, incrementMapper);
    IncrementProcessor processor2 = new IncrementProcessor(processor1, incrementMapper);
    IncrementProcessor processor3 = new IncrementProcessor(processor2, incrementMapper);

    CountDownLatch latch = new CountDownLatch(1);
    Thread.sleep(500);
    processor3.subscribe(new SystemOutSubscriber(null, ()->latch.countDown()));
    latch.await();

  }

  private void run() throws InterruptedException {
    IntegerPublisher publisher = new IntegerPublisher();
    CountDownLatch latch = new CountDownLatch(1);
    publisher.subscribe(new Flow.Subscriber<Integer>() {
      @Override
      public void onSubscribe(Flow.Subscription subscription) {
        subscription.request(Long.MAX_VALUE);
      }

      @Override
      public void onNext(Integer item) {
        System.out.println("Item: " + item);
      }

      @Override
      public void onError(Throwable throwable) {
        System.out.println("Error: " + throwable.getMessage());
      }

      @Override
      public void onComplete() {
        System.out.println("Done.");
        latch.countDown();
      }
    });
    latch.await();
  }
}
