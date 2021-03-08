package com.mageddo.tobby.internal.utils;

import java.time.Duration;

public class StopWatch {

  private Long startedAt;
  private Long stoppedAt;
  private Long splitAt;

  public StopWatch start() {
    this.startedAt = System.nanoTime();
    return this;
  }

  public StopWatch split(){
    this.splitAt = System.nanoTime();
    return this;
  }

  public Duration getDuration() {
    return Duration.ofMillis(this.getTime());
  }

  public long getTime() {
    return this.calc(this.startedAt);
  }

  public long getSplitTime(){
    return this.calc(ObjectUtils.firstNonNull(this.splitAt, this.startedAt));
  }

  public static StopWatch createStarted() {
    return new StopWatch().start();
  }

  public StopWatch stop() {
    this.stoppedAt = System.nanoTime();
    return this;
  }

  public String getDisplayTime() {
    return display(this.getTime());
  }

  public static String display(long time) {
    return String.format("%,d", time);
  }

  private long calc(Long snapshot) {
    return Duration
        .ofNanos(ObjectUtils.firstNonNull(this.stoppedAt, System.nanoTime()) - snapshot)
        .toMillis();
  }

}
