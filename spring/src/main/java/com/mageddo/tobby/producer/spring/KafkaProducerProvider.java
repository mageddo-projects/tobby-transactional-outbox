package com.mageddo.tobby.producer.spring;

import org.apache.kafka.clients.producer.Producer;
import org.springframework.kafka.core.KafkaTemplate;

@Deprecated
public interface KafkaProducerProvider {
  Producer<byte[], byte[]> createByteProducer();

  Producer<?, ?> createProducer();

  KafkaTemplate<?, ?> createKafkaTemplate();
}
