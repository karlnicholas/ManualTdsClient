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

// 3. MAIN APP
public class MockApp {

  public static void main(String[] args) {

    // --- MOCK IMPLEMENTATION ---
    Statement statement = () -> MappingProducer.just(new Result() {
      @Override
      public <R> Flow.Publisher<R> map(BiFunction<FlowRow, FlowRowMetadata, R> mapper) {
        // Simulate 2 rows coming from the DB
        return MappingProducer.from(subscriber -> {
          subscriber.onSubscribe(new Flow.Subscription() {
            @Override public void request(long n) {
              // Row 1
              subscriber.onNext(mapper.apply(new FlowRow() {
                @Override public <T> T get(int index, Class<T> type) { return type.cast("Val-" + index); }
              }, null));
              // Row 2
              subscriber.onNext(mapper.apply(new FlowRow() {
                @Override public <T> T get(int index, Class<T> type) { return type.cast("Val2-" + index); }
              }, null));
              subscriber.onComplete();
            }
            @Override public void cancel() {}
          });
        });
      }
    });

    // --- YOUR CODE BELOW ---

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