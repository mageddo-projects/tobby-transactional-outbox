package com.mageddo.tobby.producer.kafka;

import java.time.Duration;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import templates.RecordMetadataTemplates;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;

@ExtendWith(MockitoExtension.class)
class SimpleJdbcKafkaProducerAdapterTest {

  @Mock
  JdbcKafkaProducer<String, String> jdbcKafkaProducer;

  SimpleJdbcKafkaProducerAdapter<String, String> producer;

  @BeforeEach
  void beforeEach() {
    this.producer = new SimpleJdbcKafkaProducerAdapter<>(this.jdbcKafkaProducer);
  }

  @Test
  void mustSendMessageAndReturnMetadata() throws Exception {

    // arrange
    final ProducerRecord<String, String> record = new ProducerRecord<>("fruit", "Mango");
    doReturn(RecordMetadataTemplates.build())
        .when(this.jdbcKafkaProducer)
        .send(any())
    ;

    // act
    final Future<RecordMetadata> future = this.producer.send(record);

    // assert
    final RecordMetadata metadata = future.get();

    assertEquals("fruit-1@-1", metadata.toString());

  }

  @Test
  void mustSendMessageAndCallCallback() throws Exception {

    // arrange
    final ProducerRecord<String, String> record = new ProducerRecord<>("fruit", "Mango");
    doReturn(RecordMetadataTemplates.build())
        .when(this.jdbcKafkaProducer)
        .send(any())
    ;
    final AtomicReference<RecordMetadata> callbackMetadata = new AtomicReference<>();

    // act
    final Future<RecordMetadata> future = this.producer.send(record, (metadata, exception) -> {
      callbackMetadata.set(metadata);
      assertNull(exception);
    });

    // assert
    final RecordMetadata metadata = future.get();
    assertEquals("fruit-1@-1", metadata.toString());
    assertEquals("fruit-1@-1", callbackMetadata.get().toString());

  }

  @Test
  void mustClose(){

    // arrange

    // act
    this.producer.close();

    // assert
    assertTrue(this.producer.executorService.isShutdown());
    assertTrue(this.producer.executorService.isTerminated());

  }

  @Test
  void mustCloseWithTimeout(){

    // arrange

    // act
    this.producer.close(3, TimeUnit.SECONDS);

    // assert
    assertTrue(this.producer.executorService.isShutdown());
    assertTrue(this.producer.executorService.isTerminated());

  }

  @Test
  void mustCloseUsingDurationMethodSignature(){
    // arrange

    // act
    this.producer.close(Duration.ofSeconds(2));

    // assert
    assertTrue(this.producer.executorService.isShutdown());
    assertTrue(this.producer.executorService.isTerminated());
  }
}
