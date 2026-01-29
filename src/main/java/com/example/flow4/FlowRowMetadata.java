package com.example.flow4;

import java.util.List;

public interface FlowRowMetadata {
  FlowColumnMetadata getFlowColumnMetadata(int var1);

  FlowColumnMetadata getFlowColumnMetadata(String var1);

  List<? extends FlowColumnMetadata> getFlowColumnMetadatas();
}
