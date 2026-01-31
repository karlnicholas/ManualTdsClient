package com.example.flow3.library;

import java.util.concurrent.Flow;
import java.util.function.BiFunction;

// A generic version of your IncrementProcessor logic
public class MappingFunctionProcessor<T extends FlowRow, R> implements Flow.Processor<T, R> {
  private final BiFunction<T, FlowRowMetadata, ? extends R>  mapper;
  private Flow.Subscriber<? super R> downstream;

  public MappingFunctionProcessor(BiFunction<T, FlowRowMetadata, ? extends R>  mapper) {
    this.mapper = mapper;
  }

  @Override
  public void subscribe(Flow.Subscriber<? super R> subscriber) {
    System.out.println("MappingFunctionProcessor::subscribe(Flow.Subscriber<? super R> subscriber)::this.downstream = subscriber");
    this.downstream = subscriber;
  }

  @Override
  public void onSubscribe(Flow.Subscription subscription) {
    System.out.println("MappingFunctionProcessor::onSubscribe::downstream.onSubscribe(subscription)");
    downstream.onSubscribe(subscription);
  }

  @Override
  public void onNext(T flowRow) {
    System.out.println("MappingFunctionProcessor::onSubscribe::onNext(T flowRow)");
    try {
      // 1. Attempt the mapping (this executes your lambda)
      // 2. If successful, pass it downstream
      System.out.println("MappingFunctionProcessor::onSubscribe::downstream.onNext(mapper.apply(flowRow, flowRow.getFlowRowMetadata()))");
      downstream.onNext(mapper.apply(flowRow, flowRow.getFlowRowMetadata()));
    } catch (Throwable t) {
      // 3. If the lambda throws, we MUST catch it and signal onError
      // This effectively "propagates" the exception to your Client
      downstream.onError(t);
    }
  }
  @Override
  public void onError(Throwable throwable) {
    downstream.onError(throwable);
  }

  @Override
  public void onComplete() {
    System.out.println("MappingFunctionProcessor::onComplete::downstream.onComplete()");
    downstream.onComplete();
  }
}