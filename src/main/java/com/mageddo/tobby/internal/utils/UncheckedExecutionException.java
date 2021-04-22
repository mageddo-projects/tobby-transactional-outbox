package com.mageddo.tobby.internal.utils;

import java.util.concurrent.ExecutionException;

public class UncheckedExecutionException extends RuntimeException {

  private final ExecutionException executionException;

  public UncheckedExecutionException(ExecutionException executionException) {
    this.executionException = executionException;
  }

  public ExecutionException getExecutionException() {
    return executionException;
  }
}
