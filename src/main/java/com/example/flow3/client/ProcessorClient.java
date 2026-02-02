package com.example.flow3.client;

import com.example.flow3.library.*;

import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;

public class ProcessorClient {
  // 1. Create the instances ONCE as fields (or pass them in)
  FlowRowPublisher source = new FlowRowPublisher();
  FlowStatement statement = new FlowStatementImpl(source);

  public static void main(String[] args) throws InterruptedException {
    new ProcessorClient().run3();
  }

  private void run3() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    System.out.println("Invoking First: ");
    invokdeDatabase().subscribe(
        System.out::println,
        throwable -> {
          System.out.println("Error: " + throwable.getMessage());
        },
        latch::countDown
    );
    latch.await();

    latch = new CountDownLatch(1);
    System.out.println("Invoking Second: ");
    invokdeDatabase().subscribe(
        System.out::println,
        throwable -> {
          System.out.println("Error: " + throwable.getMessage());
        },
        latch::countDown
    );
    latch.await();

  }

  private MappingProducer<OutRecord> invokdeDatabase() {

    BiFunction<FlowRow, FlowRowMetadata, OutRecord> mapper = (row, rowMetadata) -> (new OutRecord(
        row.get(0, String.class),
        row.get(1, String.class),
        row.get(2, String.class)));


    return MappingProducer.from(statement.execute())
        .flatMap(result -> result.map(mapper));

  }

}