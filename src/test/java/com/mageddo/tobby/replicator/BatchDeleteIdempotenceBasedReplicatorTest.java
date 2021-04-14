package com.mageddo.tobby.replicator;

import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.TobbyConfig;

import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import templates.ProducerRecordTemplates;
import testing.DBMigration;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class BatchDeleteIdempotenceBasedReplicatorTest {

  @Mock
  Producer<byte[], byte[]> producer;

  Replicators replicator;

  TobbyConfig tobby;

  private com.mageddo.tobby.producer.Producer jdbcProducer;

  private Connection connection;

  @BeforeEach
  void beforeEach() throws SQLException {
    final var dataSource = DBMigration.migrateEmbeddedHSQLDB();
    this.connection = dataSource.getConnection();
    this.tobby = TobbyConfig.build(dataSource);
    this.jdbcProducer = tobby.producer();
    this.replicator = this.buildStrategy();
  }

  @AfterEach
  void afterEach() throws SQLException {
    this.connection.close();
  }

  @Test
  void mustSendReplicateThenDeleteRecord() {

    // arrange
    doReturn(mock(Future.class))
        .when(this.producer)
        .send(any());

    final var record = ProducerRecordTemplates.coconut();
    final var savedRecord = this.jdbcProducer.send(record);
    assertNotNull(this.findRecord(savedRecord.getId()));

    // act
    this.replicator.replicate();

    // assert
    assertNull(this.findRecord(savedRecord.getId()));
    assertNull(this.findProcessedRecord(savedRecord.getId()));

  }

  private ProducedRecord findProcessedRecord(UUID id) {
    return this.tobby.recordProcessedDAO()
        .find(this.connection, id);
  }

  private ProducedRecord findRecord(UUID id) {
    return this.tobby.recordDAO()
        .find(this.connection, id);
  }

  private Replicators buildStrategy() {
    return this.tobby.replicator(ReplicatorConfig
        .builder()
        .producer(this.producer)
        .idleTimeout(Duration.ofMillis(600))
        .idempotenceStrategy(IdempotenceStrategy.BATCH_DELETE)
        .build()
    );
  }

}
