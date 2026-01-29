package com.example.flow3;

public interface FlowReadable {
  default Object get(int index) {
    return this.get(index, Object.class);
  }

  <T> T get(int var1, Class<T> var2);

  default Object get(String name) {
    return this.get(name, Object.class);
  }

  <T> T get(String var1, Class<T> var2);
}
