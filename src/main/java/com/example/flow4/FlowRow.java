package com.example.flow4;

public interface FlowRow {

  FlowRowMetadata getFlowRowMetadata();

  default Object get(int index) {
    return this.get(index, Object.class);
  }

  <T> T get(int index, Class<T> clazz);

  default Object get(String name) {
    return this.get(name, Object.class);
  }

  <T> T get(String name, Class<T> clazz);
}
