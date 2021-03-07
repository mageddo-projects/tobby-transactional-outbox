package com.mageddo.tobby.replicator;

import java.time.Duration;
import java.util.concurrent.Future;

import com.mageddo.tobby.ParameterDAO;
import com.mageddo.tobby.ParameterDAOUniversal;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.RecordDAOHsqldb;
import com.mageddo.tobby.producer.ProducerJdbc;

import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import templates.ProducerRecordTemplates;
import testing.DBMigration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class KafkaReplicatorTest {

  @Mock
  Producer<byte[], byte[]> mockProducer;

  KafkaReplicator kafkaReplicator;

  com.mageddo.tobby.producer.Producer producer;

  @BeforeEach
  void beforeEach(){
    final var dataSource = DBMigration.migrateHSQLDB();
    RecordDAO recordDAO = new RecordDAOHsqldb();
    this.producer = new ProducerJdbc(recordDAO, dataSource);
    ParameterDAO parameterDao = new ParameterDAOUniversal();
    this.kafkaReplicator = new KafkaReplicator(
        this.mockProducer, dataSource,
        recordDAO, parameterDao,
        Duration.ofMillis(600)
    );
  }

  @Test
  void mustReplicateDataToKafka(){
    // arrange
    doReturn(mock(Future.class)).when(this.mockProducer).send(any());
    this.producer.send(ProducerRecordTemplates.strawberry());
    this.producer.send(ProducerRecordTemplates.coconut());

    // act
    this.kafkaReplicator.replicate();

    // assert
    verify(this.mockProducer, times(2)).send(any());
  }

}
