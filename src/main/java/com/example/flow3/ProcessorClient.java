package com.example.flow3;

import java.util.function.BiFunction;

public class ProcessorClient {
  public static void main(String[] args) throws InterruptedException {
    new ProcessorClient().run3();
  }

  private void run3() {
    FlowRowPublisher source = new FlowRowPublisher();
    FlowStatement statement = new FlowStatementImpl(source);

    BiFunction<FlowRow, FlowRowMetadata, OutRecord> mapper = (row, rowMetadata) -> (new OutRecord(
            row.get(0, String.class),
            row.get(1, String.class),
            row.get(2, String.class)));


    MappingProducer.from(statement.execute())
            .flatMap(result -> result.map(mapper))
            .subscribe(
                    System.out::println,
                    throwable -> {
                      System.out.println("Error: " + throwable.getMessage());
                    }
            );

  }
}