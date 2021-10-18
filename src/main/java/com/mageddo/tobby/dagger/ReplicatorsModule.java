package com.mageddo.tobby.dagger;

import javax.inject.Singleton;

import com.mageddo.tobby.Locker;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.replicator.BatchSender;
import com.mageddo.tobby.replicator.IteratorFactory;
import com.mageddo.tobby.replicator.ReplicatorConfig;
import com.mageddo.tobby.replicator.Replicators;
import com.mageddo.tobby.replicator.UpdateIdempotenceBasedReplicator;
import com.mageddo.tobby.replicator.idempotencestrategy.batchdelete.BatchParallelDeleteIdempotenceBasedReplicator;
import com.mageddo.tobby.replicator.idempotencestrategy.batchdelete.RecordDeleter;

import dagger.Module;
import dagger.Provides;

@Module
class ReplicatorsModule {

  private final ReplicatorConfig config;

  public ReplicatorsModule(ReplicatorConfig config) {
    this.config = config;
  }

  @Provides
  @Singleton
  BatchParallelDeleteIdempotenceBasedReplicator batchParallelDeleteIdempotenceBasedReplicator(
      RecordDAO recordDAO,
      BatchSender batchSender,
      BatchParallelDeleteIdempotenceBasedReplicator.Config config,
      RecordDeleter recordDeleter
  ) {
    return new BatchParallelDeleteIdempotenceBasedReplicator(
        recordDAO, this.config.getDataSource(), batchSender,
        recordDeleter, config
    );
  }

  @Provides
  @Singleton
  Replicators replicators(IteratorFactory iteratorFactory, Locker locker) {
    return new Replicators(this.config, iteratorFactory, locker);
  }

  @Provides
  @Singleton
  BatchParallelDeleteIdempotenceBasedReplicator.Config config() {
    return BatchParallelDeleteIdempotenceBasedReplicator.Config.from(this.config);
  }

  @Provides
  @Singleton
  UpdateIdempotenceBasedReplicator updateIdempotenceBasedReplicator(
      RecordDAO recordDAO,
      BatchSender batchSender,
      UpdateIdempotenceBasedReplicator.Config config
  ) {
    return new UpdateIdempotenceBasedReplicator(
        recordDAO, this.config.getDataSource(), batchSender, config
    );
  }

  @Provides
  @Singleton
  UpdateIdempotenceBasedReplicator.Config updateIdempotenceBasedReplicatorConfig() {
    return UpdateIdempotenceBasedReplicator.Config.from(this.config);
  }

  @Provides
  @Singleton
  BatchSender batchSender() {
    return new BatchSender(this.config.getProducer());
  }
}
