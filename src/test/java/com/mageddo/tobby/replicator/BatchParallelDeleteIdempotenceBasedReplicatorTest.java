package com.mageddo.tobby.replicator;

import java.time.Duration;

import com.mageddo.tobby.Tobby;

import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BatchParallelDeleteIdempotenceBasedReplicatorTest extends AbstractDeleteIdempotenceBasedReplicatorTest {

  public Replicators buildStrategy() {
    return Tobby.replicator(ReplicatorConfig
        .builder()
        .dataSource(this.dataSource)
        .producer(this.producer)
        .idleTimeout(Duration.ofMillis(600))
        .idempotenceStrategy(IdempotenceStrategy.BATCH_PARALLEL_DELETE)
        .build()
    );
  }

}
