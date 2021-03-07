package com.mageddo.tobby.replicator;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.Future;

import javax.sql.DataSource;

import com.mageddo.tobby.TobbyConfig;

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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class KafkaReplicatorTest {

  @Mock
  Producer<byte[], byte[]> mockProducer;

  KafkaReplicator replicator;

  com.mageddo.tobby.producer.Producer producer;

  TobbyConfig tobby;

  DataSource dataSource;

  @BeforeEach
  void beforeEach() {
    this.dataSource = DBMigration.migrateEmbeddedHSQLDB();
    this.tobby = TobbyConfig.build(this.dataSource);
    this.replicator = spy(this.tobby.replicator(this.mockProducer, Duration.ofMillis(600)));
    this.producer = this.tobby.producerJdbc();
  }

  @Test
  void mustReplicateDataToKafka() {
    // arrange
    doReturn(mock(Future.class)).when(this.mockProducer)
        .send(any());
    this.producer.send(ProducerRecordTemplates.strawberry());
    this.producer.send(ProducerRecordTemplates.coconut());

    // act
    this.replicator.replicate();

    // assert
    verify(this.mockProducer, times(2)).send(any());
  }

  @Test
  void mustNotProcessAlreadyAcquiredRecords() throws SQLException {
    // arrange
    doReturn(mock(Future.class)).when(this.mockProducer)
        .send(any());
    final var sent = this.producer.send(ProducerRecordTemplates.strawberry());
    this.producer.send(ProducerRecordTemplates.coconut());
    try(final var connection = this.dataSource.getConnection()){
      this.tobby.recordDAO().acquire(connection, sent.getId());
    }

    // act
    this.replicator.replicate();

    // assert
    verify(this.mockProducer, times(1)).send(any());
  }


}
