package com.mageddo.tobby;

import com.mageddo.tobby.producer.ProducerJdbc;

import dagger.Binds;
import dagger.Module;

@Module
public interface DaosProducersBindsModule {

  @Binds
  com.mageddo.tobby.producer.Producer producer(ProducerJdbc impl);

  @Binds
  RecordProcessedDAO recordProcessedDAO(RecordProcessedDAOGeneric impl);

  @Binds
  LockDAO lockDAO(LockDAOGeneric impl);

  @Binds
  ParameterDAO parameterDAO(ParameterDAOUniversal impl);


}
