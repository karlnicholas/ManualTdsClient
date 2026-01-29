package com.example.flow2;

import java.util.concurrent.Flow;

public interface FlowStatement {
  Flow.Publisher<? extends FlowResult> execute();
}
