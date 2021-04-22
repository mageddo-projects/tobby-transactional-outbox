package com.mageddo.tobby.internal.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class BatchThread<V> {

  private final List<Callable<V>> callables;

  public BatchThread() {
    this.callables = new ArrayList<>();
  }

  public BatchThread<V> add(Callable<V> callable){
    this.callables.add(callable);
    return this;
  }

  public List<Callable<V>> getCallables() {
    return this.callables;
  }
}
