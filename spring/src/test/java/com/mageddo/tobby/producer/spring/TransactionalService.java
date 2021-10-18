package com.mageddo.tobby.producer.spring;

import com.mageddo.tobby.ProducerRecord;
import com.mageddo.tobby.producer.Producer;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class TransactionalService {

  private final Producer producer;

  TransactionalService(Producer producer) {
    this.producer = producer;
  }

  @Transactional
  public void send(ProducerRecord record, int wantedInvocations) {
    for (int i = 0; i < wantedInvocations; i++) {
      this.producer.send(record);
    }
  }

}
