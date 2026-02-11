package com.example.sample;

import java.util.concurrent.CountDownLatch;

public class JavaPlay {
  public static void main(String[] args) throws InterruptedException {
    new JavaPlay().runOneShotSubscriber();
  }
  private void runOneShotSubscriber() throws InterruptedException {
    OneShotPublisher publisher = new OneShotPublisher();
    CountDownLatch latch = new CountDownLatch(1);
    publisher.subscribe(new SampleSubscriber<>(1, value->{
      System.out.println(value);
      latch.countDown();
    }));
    latch.await();
  }
}
