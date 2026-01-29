package com.example.flow2;

import java.util.concurrent.Flow;

public class FlowStatementImpl implements FlowStatement {
  private final FlowResult flowResult;
  private final Flow.Publisher<Integer> upstream;
  public FlowStatementImpl() {
    this.upstream = new IntegerPublisher();
    this.flowResult = new FlowResultImplOrig(upstream);
  }
  @Override
  public Flow.Publisher<? extends FlowResult> execute() {
    return new PassThroughProcessor(upstream);
  }
}
