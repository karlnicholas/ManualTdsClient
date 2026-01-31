package com.example.flow3.library;

import java.util.List;

public interface FlowRowMetadata {
  FlowColumnMetadata getFlowColumnMetadata(int index);

  FlowColumnMetadata getFlowColumnMetadata(String name);

  List<? extends FlowColumnMetadata> getFlowColumnMetadatas();
}
