package com.mageddo.tobby.replicator;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Future;

import javax.sql.DataSource;

import com.mageddo.tobby.ProducedRecord;
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
import templates.RecordMetadataTemplates;
import testing.DBMigration;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
class DeleteWithHistoryIdempotenceBasedReplicatorTest {

  @Mock
  Producer<byte[], byte[]> producer;

  Replicators replicator;

  TobbyFactory tobby;

  private com.mageddo.tobby.producer.Producer jdbcProducer;

  private Connection connection;
  private DataSource dataSource;

  @BeforeEach
  void beforeEach() throws SQLException {
    this.dataSource = DBMigration.migrateEmbeddedHSQLDB();
    this.connection = dataSource.getConnection();
    this.tobby = TobbyFactory.build(ProducerConfig
        .builder()
        .dataSource(this.dataSource)
        .producer(this.producer)
        .build()
    );
    this.jdbcProducer = tobby.producer();
    this.replicator = this.buildDefaultDeleteWithHistoryReplicator();
  }

  @AfterEach
  void afterEach() throws SQLException {
    this.connection.close();
  }

  @Test
  void mustSendDeleteAndTrackRecordHistory() throws Exception {

    // arrange

    final var future = mock(Future.class);
    doReturn(RecordMetadataTemplates.timestampBasedRecordMetadata())
        .when(future)
        .get();

    doReturn(future)
        .when(this.producer)
        .send(any());

    doReturn(future)
        .when(this.producer)
        .send(any(), any());


    final var record = ProducerRecordTemplates.coconut();
    final var savedRecord = this.jdbcProducer.send(record);
    assertNotNull(this.findRecord(savedRecord.getId()));


    // act
    this.replicator.replicate();

    // assert
    assertNull(this.findRecord(savedRecord.getId()));
    assertNotNull(this.findProcessedRecord(savedRecord.getId()));

  }

  private ProducedRecord findProcessedRecord(UUID id) {
    return this.tobby.recordProcessedDAO()
        .find(this.connection, id);
  }

  private ProducedRecord findRecord(UUID id) {
    return this.tobby.recordDAO()
        .find(this.connection, id);
  }

  private Replicators buildDefaultDeleteWithHistoryReplicator() {
    return Tobby.replicator(ReplicatorConfig
        .builder()
        .producer(this.producer)
        .dataSource(this.dataSource)
        .idleTimeout(Duration.ofMillis(600))
        .idempotenceStrategy(IdempotenceStrategy.DELETE_WITH_HISTORY)
        .build()
    );
  }

}
