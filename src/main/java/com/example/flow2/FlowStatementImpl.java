package com.example.flow2;

import java.util.concurrent.Flow;

public class FlowStatementImpl implements FlowStatement {
  @Override
  public Flow.Publisher<FlowResult> execute() {
    // We return a Publisher that emits a single FlowResult
    return subscriber -> {
      subscriber.onSubscribe(new Flow.Subscription() {
        @Override
        public void request(long n) {
          if (n > 0) {
            subscriber.onNext(new FlowResultImpl());
            subscriber.onComplete();
          }
        }
        @Override
        public void cancel() {}
      });
    };
  }
}