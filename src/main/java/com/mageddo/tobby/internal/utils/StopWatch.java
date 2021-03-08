package com.mageddo.tobby.internal.utils;

import java.time.Duration;

public class StopWatch {

  private Long startedAt;
  private Long stoppedAt;

  public StopWatch start() {
    this.startedAt = System.nanoTime();
    return this;
  }

  public Duration getDuration() {
    return Duration.ofMillis(this.getTime());
  }

  public long getTime() {
    return Duration
        .ofNanos(ObjectUtils.firstNonNull(this.stoppedAt, System.nanoTime()) - this.startedAt)
        .toMillis();
  }

  public static StopWatch createStarted() {
    return new StopWatch().start();
  }

  public StopWatch stop() {
    this.stoppedAt = System.nanoTime();
    return this;
  }
}
