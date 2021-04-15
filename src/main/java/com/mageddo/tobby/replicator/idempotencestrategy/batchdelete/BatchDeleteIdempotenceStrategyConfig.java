package com.mageddo.tobby.replicator.idempotencestrategy.batchdelete;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
public class BatchDeleteIdempotenceStrategyConfig {
  @NonNull
  @Builder.Default
  private final DeleteMode deleteMode = DeleteMode.BATCH_DELETE_USING_THREADS;

  public static BatchDeleteIdempotenceStrategyConfig defaultConfig() {
    return BatchDeleteIdempotenceStrategyConfig
        .builder()
        .build();
  }
}
