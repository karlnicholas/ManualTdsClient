package com.example.flow4;

import java.util.concurrent.Flow;
import java.util.function.Function;

public interface FlowResult {

  <T> Flow.Publisher<T> map(Function<Integer, ? extends T> mappingFunction);

  <T> Flow.Publisher<T> flatMap(Function<Integer, ? extends Flow.Publisher<? extends T>> mappingFunction);

}