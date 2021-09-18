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
import com.mageddo.tobby.ProducedRecord.Status;
import com.mageddo.tobby.Tobby;
import com.mageddo.tobby.dagger.TobbyFactory;
import com.mageddo.tobby.producer.ProducerConfig;

import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import templates.ProducerRecordTemplates;
import testing.DBMigration;

import static com.mageddo.tobby.replicator.ReplicatorConfig.REPLICATORS_BATCH_PARALLEL_BUFFER_SIZE;
import static com.mageddo.tobby.replicator.ReplicatorConfig.REPLICATORS_BATCH_PARALLEL_THREAD_BUFFER_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class UpdateIdempotenceBasedReplicatorTest {

  @Mock
  Producer<byte[], byte[]> producer;

  Replicators replicator;

  TobbyFactory tobby;

  com.mageddo.tobby.producer.Producer jdbcProducer;

  Connection connection;

  DataSource dataSource;

  @BeforeEach
  void beforeEach() throws SQLException {
    this.dataSource = DBMigration.migrateEmbeddedHSQLDB();
    this.connection = this.dataSource.getConnection();
    this.tobby = TobbyFactory.build(ProducerConfig
        .builder()
        .dataSource(this.dataSource)
        .producer(this.producer)
        .build()
    );
    this.jdbcProducer = tobby.producer();
    this.replicator = this.buildStrategy();
  }

  @AfterEach
  void afterEach() throws SQLException {
    this.connection.close();
  }

  @Test
  void mustNotSendNorReplicateWhenWaitTimeIsNotMet() {

    // arrange
    this.replicator = Tobby
        .replicator(this.defaultConfigBuilder()
            .build()
        );

    final var record = ProducerRecordTemplates.coconut();
    final var savedRecord = this.jdbcProducer.send(record);
    assertNotNull(this.findRecord(savedRecord.getId()));

    // act
    this.replicator.replicate();

    // assert
    verify(this.producer, never()).send(any());

    final var foundRecord = this.findRecord(savedRecord.getId());
    assertNotNull(foundRecord);
    assertEquals(Status.WAIT, foundRecord.getStatus());

    assertNull(this.findProcessedRecord(savedRecord.getId()));

  }

  @Test
  void mustSendReplicateThenUpdateRecord() {

    // arrange
    doReturn(mock(Future.class))
        .when(this.producer)
        .send(any());

    doReturn(mock(Future.class))
        .when(this.producer)
        .send(any(), any());

    final var record = ProducerRecordTemplates.coconut();
    final var savedRecord = this.jdbcProducer.send(record);
    assertNotNull(this.findRecord(savedRecord.getId()));

    // act
    this.replicator.replicate();

    // assert
    verify(this.producer).send(any());

    final var foundRecord = this.findRecord(savedRecord.getId());
    assertNotNull(foundRecord);
    assertEquals(Status.OK, foundRecord.getStatus());

    assertNull(this.findProcessedRecord(savedRecord.getId()));

  }

  @Test
  void mustSendReplicateThenUpdateRecords() {

    // arrange
    doReturn(mock(Future.class))
        .when(this.producer)
        .send(any());

    doReturn(mock(Future.class))
        .when(this.producer)
        .send(any(), any());

    final int count = 1001;

    final List<UUID> ids = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      final var record = ProducerRecordTemplates.coconut();
      final var savedRecord = this.jdbcProducer.send(record);
      final var foundRecord = this.findRecord(savedRecord.getId());
      assertNotNull(foundRecord);
      assertEquals(Status.WAIT, foundRecord.getStatus());
      ids.add(savedRecord.getId());
    }

    // act
    this.replicator.replicate();

    // assert
    int i = 0;
    for (UUID id : ids) {
      final var foundRecord = this.findRecord(id);
      assertNotNull(foundRecord);
      assertEquals(Status.OK, foundRecord.getStatus(), String.valueOf(i));
      assertNull(this.findProcessedRecord(id));
      i++;
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


  public Replicators buildStrategy() {
    return Tobby.replicator(defaultConfigBuilder()
        .put(ReplicatorConfig.REPLICATORS_UPDATE_IDEMPOTENCE_TIME_TO_WAIT_BEFORE_REPLICATE, "PT0S")
        .build());
  }

  private ReplicatorConfig.ReplicatorConfigBuilder defaultConfigBuilder() {
    return ReplicatorConfig
        .builder()
        .dataSource(this.dataSource)
        .producer(this.producer)
        .idempotenceStrategy(IdempotenceStrategy.BATCH_PARALLEL_UPDATE)
        .put(REPLICATORS_BATCH_PARALLEL_BUFFER_SIZE, "500")
        .put(REPLICATORS_BATCH_PARALLEL_THREAD_BUFFER_SIZE, "100")
        .idleTimeout(Duration.ofMillis(600))
        ;
  }

}
