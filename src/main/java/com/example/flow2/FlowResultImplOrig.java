package com.example.flow2;

import java.util.concurrent.Flow;
import java.util.function.Function;

public class FlowResultImplOrig implements FlowResult {
  private final Flow.Publisher<Integer> source;

  public FlowResultImplOrig(Flow.Publisher<Integer> source) {
    this.source = source;
  }

  @Override
  public <T> Flow.Publisher<T> map(Function<Integer, ? extends T> mappingFunction) {
    // Return a new Publisher. When someone subscribes to THIS...
    return subscriber -> {
      // create a mapping processor, give it the mapping function and the subscriber
      MappingFunctionProcessor<Integer, T> processor = new MappingFunctionProcessor<>(mappingFunction);
      // 1. First, tell the processor who the final subscriber is
      processor.subscribe(subscriber);
      // 2. ONLY THEN, connect the processor to the data source
      source.subscribe(processor);
    };
  }
}