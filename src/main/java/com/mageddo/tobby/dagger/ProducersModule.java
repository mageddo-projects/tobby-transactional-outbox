package com.mageddo.tobby.dagger;

import javax.inject.Singleton;

import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.producer.ProducerConfig;
import com.mageddo.tobby.producer.ProducerEventualConsistent;
import com.mageddo.tobby.producer.ProducerJdbc;

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
  public ProducerEventualConsistent producerEventualConsistent(RecordDAO recordDAO) {
    return new ProducerEventualConsistent(
        this.buildProducer(),
        recordDAO,
        this.producerConfig.getDataSource()
    );
  }

  private Producer<byte[], byte[]> buildProducer() {
    if (this.producerConfig.getProducer() != null) {
      return this.producerConfig.getProducer();
    }
    return new KafkaProducer<>(this.producerConfig.getProducerConfigs());
  }

}
