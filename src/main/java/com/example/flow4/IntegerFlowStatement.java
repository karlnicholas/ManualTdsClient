package com.example.flow4;

import java.util.concurrent.Flow;

public class IntegerFlowStatement implements FlowStatement {
  @Override
  public Flow.Publisher<FlowResult> execute() {
    // We return a Publisher that emits a single FlowResult
    return subscriber -> {
      subscriber.onSubscribe(new Flow.Subscription() {
        @Override
        public void request(long n) {
          if (n > 0) {
            subscriber.onNext(new IntegerFlowResult());
            subscriber.onComplete();
          }
        }
        @Override
        public void cancel() {}
      });
    };
  }
}