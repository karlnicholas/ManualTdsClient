package com.example.flow3.library;

import java.util.concurrent.Flow;
import java.util.function.BiFunction;

public interface FlowResult {
  <T> Flow.Publisher<T> map(BiFunction<FlowRow, FlowRowMetadata, ? extends T> mappingFunction);
}