package com.mageddo.tobby.producer;

import com.mageddo.tobby.producer.spring.KafkaProducerProvider;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.kafka.core.KafkaTemplate;

@Deprecated
public class MockKafkaProducerProvider implements KafkaProducerProvider {

  private final MockProducer mockProducer = new MockProducer();

  @Override
  public Producer<byte[], byte[]> createByteProducer() {
    return this.mockProducer;
  }

  @Override
  public Producer<?, ?> createProducer() {
    return null;
  }

  @Override
  public KafkaTemplate<?, ?> createKafkaTemplate() {
    return null;
  }

  public MockProducer getMockProducer() {
    return mockProducer;
  }
}
