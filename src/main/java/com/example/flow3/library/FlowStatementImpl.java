package com.example.flow3.library;

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
          System.out.println("FlowStatementImpl::InnerSubscriber::Request " + n + " rows");
          // Use a boolean guard to ensure we run only once
          if (!completed && n > 0) {
            completed = true;
            System.out.println("FlowStatementImpl::InnerSubscriber::onNext:");
            subscriber.onNext(new FlowResultImpl(source));
            System.out.println("FlowStatementImpl::InnerSubscriber::onComplete:");
            subscriber.onComplete();
          }
        }
        @Override
        public void cancel() {}
      });
    };
  }
}