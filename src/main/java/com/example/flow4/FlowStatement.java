package com.example.flow4;

import java.util.concurrent.Flow;

public interface FlowStatement {
  Flow.Publisher<? extends FlowResult> execute();
}
