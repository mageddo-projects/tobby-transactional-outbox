package com.mageddo.tobby.internal.utils;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class SyncFuture<V> implements Future<V> {

  private final V value;

  public SyncFuture(V value) {
    this.value = value;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return true;
  }

  @Override
  public V get() {
    return this.value;
  }

  @Override
  public V get(long timeout, TimeUnit unit) {
    return this.value;
  }
}
