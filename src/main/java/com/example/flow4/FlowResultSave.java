package com.example.flow4;

import java.util.concurrent.Flow;
import java.util.function.Function;

public interface FlowResultSave {

  <T> Flow.Publisher<T> map(Function<Integer, ? extends T> mappingFunction);

}