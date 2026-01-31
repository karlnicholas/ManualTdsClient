package com.example.flow3.library;

import java.util.List;

public class FlowRowMetadataImpl implements FlowRowMetadata {
  private final List<String> columnNames;

  public FlowRowMetadataImpl(List<String> columnNames) {
    this.columnNames = columnNames;
  }

  @Override
  public FlowColumnMetadata getFlowColumnMetadata(int index) {
    return new FlowColumnMetadataImpl(columnNames.get(index));
  }

  @Override
  public FlowColumnMetadata getFlowColumnMetadata(String name) {
    for (FlowColumnMetadata columnMetadata : getFlowColumnMetadatas()) {
      if (columnMetadata.getName().equals(name)) {
        return columnMetadata;
      }
    }
    throw new IllegalArgumentException("Column not found: " + name);
  }

  @Override
  public List<? extends FlowColumnMetadata> getFlowColumnMetadatas() {
    return columnNames.stream().map(FlowColumnMetadataImpl::new).toList();
  }
}
