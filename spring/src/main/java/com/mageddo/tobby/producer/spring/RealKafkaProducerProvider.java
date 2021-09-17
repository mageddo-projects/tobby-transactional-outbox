package com.mageddo.tobby.producer.spring;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

public class RealKafkaProducerProvider {

  private final KafkaProperties kafkaProperties;

  public RealKafkaProducerProvider(
      KafkaProperties kafkaProperties
  ) {
    this.kafkaProperties = kafkaProperties;
  }

  public Producer<byte[], byte[]> createByteProducer() {
    return new KafkaProducer<>(
        this.kafkaProperties.buildProducerProperties(),
        new ByteArraySerializer(),
        new ByteArraySerializer()
    );
  }

  public Producer<?, ?> createProducer() {
    return new KafkaProducer<>(this.kafkaProperties.buildProducerProperties());
  }

  public KafkaTemplate<?, ?> createKafkaTemplate() {
    return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(this.kafkaProperties.buildProducerProperties()));
  }
}
