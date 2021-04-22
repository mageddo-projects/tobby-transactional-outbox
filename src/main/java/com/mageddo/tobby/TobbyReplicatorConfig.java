package com.mageddo.tobby;

import com.mageddo.tobby.replicator.BatchSender;
import com.mageddo.tobby.replicator.IteratorFactory;
import com.mageddo.tobby.replicator.ReplicatorConfig;
import com.mageddo.tobby.replicator.Replicators;
import com.mageddo.tobby.replicator.idempotencestrategy.batchdelete.BatchParallelDeleteIdempotenceBasedReplicator;
import com.mageddo.tobby.replicator.idempotencestrategy.batchdelete.RecordDeleter;

import dagger.Component;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Singleton
@Component(
    modules = {
        TobbyReplicatorConfig.ReplicatorsModule.class,
        DaosProducersModule.class,
        DaosProducersBindsModule.class
    }
)
public interface TobbyReplicatorConfig {

  Replicators replicators();

  @Module
  public static class ReplicatorsModule {

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
    BatchParallelDeleteIdempotenceBasedReplicator.Config config(){
      return BatchParallelDeleteIdempotenceBasedReplicator.Config.from(this.config);
    }

    @Provides
    @Singleton
    BatchSender batchSender(){
      return new BatchSender(this.config.getProducer());
    }
  }

  static Replicators create(ReplicatorConfig replicatorConfig) {
    return DaggerTobbyReplicatorConfig
        .builder()
        .replicatorsModule(new ReplicatorsModule(replicatorConfig))
        .daosProducersModule(new DaosProducersModule(replicatorConfig.getDataSource()))
        .build()
        .replicators();
  }
}
