package com.example.flow3;

import java.util.concurrent.Flow;

public class FlowStatementImpl implements FlowStatement {
  private final FlowRowPublisher source;
  private boolean completed;

  public FlowStatementImpl(FlowRowPublisher source) {
    this.source = source;
    this.completed = false;
  }

  @Override
  public Flow.Publisher<FlowResult> execute() {
    // We return a Publisher that emits a single FlowResult
    return subscriber -> {
      subscriber.onSubscribe(new Flow.Subscription() {
        @Override
        public void request(long n) {
          // Use a boolean guard to ensure we run only once
          if (!completed && n > 0) {
            completed = true;
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