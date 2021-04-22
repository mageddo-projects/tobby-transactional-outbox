package com.mageddo.tobby.dagger;

import javax.inject.Singleton;

import com.mageddo.tobby.replicator.ReplicatorConfig;
import com.mageddo.tobby.replicator.Replicators;

import dagger.Component;

@Singleton
@Component(
    modules = {
        ReplicatorsModule.class,
        DaosProducersModule.class,
        DaosProducersBindsModule.class
    }
)
public interface TobbyReplicatorConfig {

  Replicators replicators();

  static Replicators create(ReplicatorConfig replicatorConfig) {
    return DaggerTobbyReplicatorConfig
        .builder()
        .replicatorsModule(new ReplicatorsModule(replicatorConfig))
        .daosProducersModule(new DaosProducersModule(replicatorConfig.getDataSource()))
        .build()
        .replicators();
  }
}
