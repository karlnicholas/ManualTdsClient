package com.example.flow4;

public class FlowColumnMetadataImpl implements FlowColumnMetadata {
  private final String name;
  public FlowColumnMetadataImpl(String name) {
    this.name = name;
  }
  @Override
  public String getName() {
    return name;
  }
}
