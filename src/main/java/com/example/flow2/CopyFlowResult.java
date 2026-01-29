package com.example.flow2;

import io.r2dbc.spi.Readable;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;

import java.util.concurrent.Flow;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface CopyFlowResult {

  <T> Flow.Publisher<T> map(BiFunction<Row, RowMetadata, ? extends T> mappingFunction);

  default <T> Flow.Publisher<T> map(Function<? super Readable, ? extends T> mappingFunction) {
    return map((row, metadata) -> mappingFunction.apply(row));
  }

}
