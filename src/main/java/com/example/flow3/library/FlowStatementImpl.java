package com.example.flow3.library;

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
        // FIXED: Now every subscriber gets their own fresh flag
        private boolean completed = false;
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