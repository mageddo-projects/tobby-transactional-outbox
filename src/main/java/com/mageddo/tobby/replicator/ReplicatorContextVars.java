package com.mageddo.tobby.replicator;

import java.time.Duration;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Value;

@Value
@Builder(access = AccessLevel.PACKAGE)
public class ReplicatorContextVars {
  /**
   * Records processed in the last wave.
   */
  private int waveProcessed;

  /**
   * The duration of the last wave
   */
  private Duration waveDuration;

  /**
   * Time since some wave processed at least one record
   */
  private Duration durationSinceLastTimeProcessed;

  /**
   * Last wave number, it's a counter starting from 1
   */
  private int wave;
}
