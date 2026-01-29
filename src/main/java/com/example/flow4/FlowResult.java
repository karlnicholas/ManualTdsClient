package com.example.flow4;

import java.util.concurrent.Flow;
import java.util.function.BiFunction;

public interface FlowResult {
  <T> Flow.Publisher<T> map(BiFunction<FlowRow, FlowRowMetadata, ? extends T> mappingFunction);
}