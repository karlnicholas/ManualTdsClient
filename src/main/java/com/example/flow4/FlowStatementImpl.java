package com.example.flow4;

import java.util.concurrent.Flow;

public class FlowStatementImpl implements FlowStatement {
  private final FlowRowPublisher source;

  public FlowStatementImpl(FlowRowPublisher source) {
    this.source = source;
  }

  @Override
  public Flow.Publisher<FlowResult> execute() {
    // We return a Publisher that emits a single FlowResult
    return subscriber -> {
      subscriber.onSubscribe(new Flow.Subscription() {
        @Override
        public void request(long n) {
          if (n > 0) {
            subscriber.onNext(new FlowResultImpl(source));
            subscriber.onComplete();
          }
        }
        @Override
        public void cancel() {}
      });
    };
  }
}