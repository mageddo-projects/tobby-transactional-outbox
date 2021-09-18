package com.mageddo.tobby.producer.kafka;

import com.mageddo.tobby.producer.InterceptableProducer;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import templates.ProducedRecordTemplates;
import templates.KafkaProducerRecordTemplates;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class JdbcKafkaProducerTest {

  @Mock
  InterceptableProducer delegate;

  JdbcKafkaProducer<String, byte[]> jdbcKafkaProducer;

  @BeforeEach
  void beforeEach(){
    this.jdbcKafkaProducer = new JdbcKafkaProducer<>(this.delegate, new StringSerializer(), new ByteArraySerializer());
  }

  @Test
  void mustConvertAndProduceRecord(){
    // arrange
    final var fruit = KafkaProducerRecordTemplates.coconut();
    doReturn(ProducedRecordTemplates.coconut()).when(this.delegate).send(any());

    // act
    this.jdbcKafkaProducer.send(fruit);

    // assert
    verify(this.delegate).send(any());
  }

}
