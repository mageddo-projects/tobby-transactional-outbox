package com.mageddo.tobby.dagger;

import javax.inject.Singleton;

import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.producer.ProducerConfig;
import com.mageddo.tobby.producer.ProducerEventualConsistent;

import org.apache.kafka.clients.producer.KafkaProducer;

import dagger.Module;
import dagger.Provides;

@Module
class ProducersModule {

  private final ProducerConfig producerConfig;

  ProducersModule(ProducerConfig producerConfig) {
    this.producerConfig = producerConfig;
  }

  @Provides
  @Singleton
  public ProducerEventualConsistent producerJdbc(RecordDAO recordDAO) {
    return new ProducerEventualConsistent(
        new KafkaProducer<>(this.producerConfig.getProducerConfigs()),
        recordDAO,
        this.producerConfig.getDataSource()
    );
  }

}
