package com.mageddo.tobby.replicator;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;

import javax.sql.DataSource;

import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.dagger.TobbyFactory;

import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import templates.ProducerRecordTemplates;
import templates.RecordMetadataTemplates;
import testing.DBMigration;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

abstract class AbstractDeleteIdempotenceBasedReplicatorTest {

  @Mock
  Producer<byte[], byte[]> producer;

  Replicators replicator;

  TobbyFactory tobby;

  com.mageddo.tobby.producer.Producer jdbcProducer;

  Connection connection;

  DataSource dataSource;

  abstract Replicators buildStrategy();

  @BeforeEach
  void beforeEach() throws SQLException {
    this.dataSource = DBMigration.migrateEmbeddedHSQLDB();
    this.connection = this.dataSource.getConnection();
    this.tobby = TobbyFactory.build(this.dataSource);
    this.jdbcProducer = tobby.producer();
    this.replicator = this.buildStrategy();
  }

  @AfterEach
  void afterEach() throws SQLException {
    this.connection.close();
  }

  @Test
  void mustSendReplicateThenDeleteRecord() throws Exception {

    // arrange
    final var future = mock(Future.class);
    doReturn(RecordMetadataTemplates.timestampBasedRecordMetadata())
        .when(future)
        .get();

    doReturn(future)
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

  @Test
  void mustSendReplicateThenDeleteRecords() throws Exception {

    // arrange
    final var future = mock(Future.class);
    doReturn(RecordMetadataTemplates.timestampBasedRecordMetadata())
        .when(future)
        .get();

    doReturn(future)
        .when(this.producer)
        .send(any());

    final int count = 1001;

    final List<UUID> ids = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      final var record = ProducerRecordTemplates.coconut();
      final var savedRecord = this.jdbcProducer.send(record);
      assertNotNull(this.findRecord(savedRecord.getId()));
      ids.add(savedRecord.getId());
    }

    // act
    this.replicator.replicate();

    // assert
    for (UUID id : ids) {
      assertNull(this.findRecord(id));
      assertNull(this.findProcessedRecord(id));
    }

  }

  private ProducedRecord findProcessedRecord(UUID id) {
    return this.tobby.recordProcessedDAO()
        .find(this.connection, id);
  }

  private ProducedRecord findRecord(UUID id) {
    return this.tobby.recordDAO()
        .find(this.connection, id);
  }


}
