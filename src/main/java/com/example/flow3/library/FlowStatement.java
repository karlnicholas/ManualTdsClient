package com.example.flow3.library;

import java.util.concurrent.Flow;

public interface FlowStatement {
  Flow.Publisher<? extends FlowResult> execute();
}
