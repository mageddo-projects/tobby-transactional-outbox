package com.mageddo.tobby.replicator;

import java.time.Duration;
import java.util.function.Predicate;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ReplicatorStopPredicates {

  public static Predicate<ReplicatorContextVars> notProcessedAtLeastOneHundredRecords() {
    return (it) -> it.getWaveProcessed() < 100;
  }

  public static Predicate<ReplicatorContextVars> tookLessThanOneSecond() {
    return (it) -> it.getWaveDuration()
        .compareTo(Duration.ofSeconds(1)) <= 0;
  }

  public static Predicate<ReplicatorContextVars> exitWhenReplicatingFasterThanProducing() {
    return notProcessedAtLeastOneHundredRecords().and(tookLessThanOneSecond());
  }
}
