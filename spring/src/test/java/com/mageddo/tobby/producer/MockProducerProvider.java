package com.mageddo.tobby.producer;

import org.apache.kafka.clients.producer.MockProducer;

public class MockProducerProvider {

  private final MockProducer producer;

  public MockProducerProvider(){
    this.producer = new MockProducer();
  }

  public MockProducerProvider(MockProducer producer) {
    this.producer = producer;
  }

  public MockProducer get(){
    return this.producer;
  }
}
