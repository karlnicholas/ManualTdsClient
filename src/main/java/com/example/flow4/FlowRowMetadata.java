package com.example.flow4;

import java.util.List;

public interface FlowRowMetadata {
  FlowColumnMetadata getFlowColumnMetadata(int index);

  FlowColumnMetadata getFlowColumnMetadata(String name);

  List<? extends FlowColumnMetadata> getFlowColumnMetadatas();
}
