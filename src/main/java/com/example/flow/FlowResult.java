package com.example.flow;

import java.util.concurrent.Flow;
import java.util.function.Function;

public interface FlowResult {

  <T> Flow.Publisher<T> map(Function<Integer, ? extends T> mappingFunction);
}