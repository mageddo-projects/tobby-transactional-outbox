package com.mageddo.tobby.dagger;

import javax.inject.Singleton;

import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.producer.ProducerConfig;
import com.mageddo.tobby.producer.ProducerEventuallyConsistent;
import com.mageddo.tobby.producer.ProducerJdbc;

import com.mageddo.tobby.producer.jmx.TobbyProducerJMX;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

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
  public ProducerJdbc producerJdbc(RecordDAO recordDAO) {
    return new ProducerJdbc(recordDAO, this.producerConfig.getDataSource());
  }

  @Provides
  @Singleton
  public ProducerEventuallyConsistent producerEventualConsistent(RecordDAO recordDAO, Producer<byte[], byte[]> producer) {
    return new ProducerEventuallyConsistent(
        producer, recordDAO,
        this.producerConfig.getDataSource()
    );
  }

  @Provides
  @Singleton
  public Producer<byte[], byte[]> buildProducer() {
    if (this.producerConfig.getProducer() != null) {
      return this.producerConfig.getProducer();
    }
    return new KafkaProducer<>(this.producerConfig.getProducerConfigs());
  }

  @Provides
  @Singleton
  public TobbyProducerJMX producerJMX(RecordDAO recordDAO, com.mageddo.tobby.producer.Producer producer){
    return new TobbyProducerJMX(recordDAO, this.producerConfig.getDataSource(), producer);
  }

}
