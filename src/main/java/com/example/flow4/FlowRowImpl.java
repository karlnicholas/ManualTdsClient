package com.example.flow4;

import java.util.List;

public class FlowRowImpl implements FlowRow {
  private final List<byte[]> row;
  private final List<String> columnNames;

  public FlowRowImpl(List<byte[]> row, List<String> columnNames) {
    this.row = row;
    this.columnNames = columnNames;
  }

  @Override
  public FlowRowMetadata getFlowRowMetadata() {
    return new FlowRowMetadataImpl(columnNames);
  }

  @Override
  public <T extends Object> T get(int index, Class<T> clazz) {
    String i = Integer.valueOf(row.get(index)[0]).toString();
    T returnValue = null;
    try {
      returnValue = clazz.cast(i);
    } catch (ClassCastException e) {
      System.out.println("ClassCastException: " + e.getMessage());
    }
    return returnValue;
  }

  @Override
  public <T> T get(String name, Class<T> clazz) {
    for(int i = 0; i < columnNames.size(); i++) {
      if(columnNames.get(i).equals(name)) {
        return clazz.cast(Integer.valueOf(row.get(i)[0]));
      }
    }
    throw new IllegalArgumentException("Column not found: " + name);
  }
}
