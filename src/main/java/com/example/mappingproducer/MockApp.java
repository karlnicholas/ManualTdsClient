package com.example.mappingproducer;

import java.util.concurrent.Flow;
import java.util.function.BiFunction;

// 1. DUMMY DATA STRUCTURES
class OutRecord {
  String c1, c2, c3;
  public OutRecord(String c1, String c2, String c3) {
    this.c1 = c1; this.c2 = c2; this.c3 = c3;
  }
  @Override public String toString() { return "Record[" + c1 + ", " + c2 + ", " + c3 + "]"; }
}

interface FlowRowMetadata {}

interface FlowRow {
  <T> T get(int index, Class<T> type);
}

// 2. THE RESULT INTERFACE (The Missing Link)
interface Result {
  // This allows result.map(mapper) to work
  <R> Flow.Publisher<R> map(BiFunction<FlowRow, FlowRowMetadata, R> f);
}

interface Statement {
  Flow.Publisher<Result> execute();
}

