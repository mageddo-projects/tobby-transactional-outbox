package com.mageddo.tobby.dagger;

import com.mageddo.tobby.LockDAO;
import com.mageddo.tobby.LockDAOGeneric;
import com.mageddo.tobby.ParameterDAO;
import com.mageddo.tobby.ParameterDAOUniversal;
import com.mageddo.tobby.RecordProcessedDAO;
import com.mageddo.tobby.RecordProcessedDAOGeneric;
import com.mageddo.tobby.producer.ProducerEventualConsistent;

import dagger.Binds;
import dagger.Module;

@Module
interface DaosProducersBindsModule {

  @Binds
  com.mageddo.tobby.producer.Producer producer(ProducerEventualConsistent impl);

  @Binds
  RecordProcessedDAO recordProcessedDAO(RecordProcessedDAOGeneric impl);

  @Binds
  LockDAO lockDAO(LockDAOGeneric impl);

  @Binds
  ParameterDAO parameterDAO(ParameterDAOUniversal impl);


}
