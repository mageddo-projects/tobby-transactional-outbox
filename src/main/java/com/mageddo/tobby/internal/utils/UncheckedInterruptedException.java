package com.mageddo.tobby.internal.utils;

public class UncheckedInterruptedException extends RuntimeException {

  private final InterruptedException interruptedException;

  public UncheckedInterruptedException(InterruptedException interruptedException) {
    this.interruptedException = interruptedException;
  }

  public InterruptedException getInterruptedException() {
    return interruptedException;
  }
}
