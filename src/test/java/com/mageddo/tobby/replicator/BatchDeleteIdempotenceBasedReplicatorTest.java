package com.mageddo.tobby.replicator;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;

import javax.sql.DataSource;

import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.Tobby;
import com.mageddo.tobby.TobbyConfig;
import com.mageddo.tobby.replicator.idempotencestrategy.batchdelete.BatchDeleteIdempotenceStrategyConfig;
import com.mageddo.tobby.replicator.idempotencestrategy.batchdelete.DeleteMode;

import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import templates.ProducerRecordTemplates;
import testing.DBMigration;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
class BatchDeleteIdempotenceBasedReplicatorTest {

  @Mock
  Producer<byte[], byte[]> producer;

  Replicators replicator;

  TobbyConfig tobby;

  private com.mageddo.tobby.producer.Producer jdbcProducer;

  private Connection connection;

  private DataSource dataSource;

  @BeforeEach
  void beforeEach() throws SQLException {
    this.dataSource = DBMigration.migrateEmbeddedHSQLDB();
    this.connection = this.dataSource.getConnection();
    this.tobby = TobbyConfig.build(this.dataSource);
    this.jdbcProducer = tobby.producer();
    this.replicator = this.buildStrategy(DeleteMode.BATCH_DELETE);
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

  @Test
  void mustSendReplicateThenDeleteRecords() {

    // arrange
    doReturn(mock(Future.class))
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

  @Test
  void mustSendReplicateThenDeleteRecordsUsingDeleteUsingThreadsMode() {

    // arrange
    this.replicator = this.buildStrategy(DeleteMode.BATCH_DELETE_USING_THREADS);
    doReturn(mock(Future.class))
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

  @Test
  void mustSendReplicateThenDeleteRecordsUsingDeleteUsingInMode() {

    // arrange
    this.replicator = this.buildStrategy(DeleteMode.BATCH_DELETE_USING_IN);
    doReturn(mock(Future.class))
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

  private Replicators buildStrategy(DeleteMode deleteMode) {
    return Tobby.replicator(ReplicatorConfig
        .builder()
        .dataSource(this.dataSource)
        .producer(this.producer)
        .idleTimeout(Duration.ofMillis(600))
        .idempotenceStrategy(IdempotenceStrategy.BATCH_DELETE)
        .deleteIdempotenceStrategyConfig(BatchDeleteIdempotenceStrategyConfig
            .builder()
            .deleteMode(deleteMode)
            .build()
        )
        .build()
    );
  }

}
