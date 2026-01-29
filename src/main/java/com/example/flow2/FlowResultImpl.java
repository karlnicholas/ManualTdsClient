package com.example.flow2;

import java.util.concurrent.Flow;
import java.util.function.Function;

public class FlowResultImpl implements FlowResult {
  @Override
  public <T> Flow.Publisher<T> map(Function<Integer, ? extends T> mappingFunction) {
    return subscriber -> {
      IntegerPublisher source = new IntegerPublisher();
      // We need a middle-man (Processor) to apply the mapping function
      source.subscribe(new MappingSubscriber<>(subscriber, mappingFunction));
    };
  }
}
