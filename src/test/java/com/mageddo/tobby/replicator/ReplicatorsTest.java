package com.mageddo.tobby.replicator;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import com.mageddo.tobby.Tobby;
import com.mageddo.tobby.dagger.TobbyFactory;
import com.mageddo.tobby.internal.utils.Threads;
import com.mageddo.tobby.producer.ProducerConfig;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import lombok.SneakyThrows;
import templates.ProducerRecordTemplates;
import templates.RecordMetadataTemplates;
import testing.DBMigration;
import testing.PostgresExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith({PostgresExtension.class, MockitoExtension.class})
class ReplicatorsTest {

  public static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofMillis(600);

  @Mock
  Producer<byte[], byte[]> mockProducer;

  com.mageddo.tobby.producer.Producer producer;

  TobbyFactory tobby;

  DataSource dataSource;

  @BeforeEach
  void beforeEach() {
    this.dataSource = DBMigration.migrateEmbeddedPostgres();
    this.tobby = TobbyFactory.build(ProducerConfig
        .builder()
        .dataSource(this.dataSource)
        .producer(this.mockProducer)
        .build()
    );
    this.producer = this.tobby.jdbcProducer();
  }

  @Test
  void mustStopWhenZeroRecordsWereProcessedOnWave() throws Exception {
    // arrange
    final Future<RecordMetadata> future = mock(Future.class);
    doReturn(RecordMetadataTemplates.timestampBasedRecordMetadata())
        .when(future)
        .get();

    doReturn(future)
        .when(this.mockProducer)
        .send(any());

    final var send = this.producer.send(ProducerRecordTemplates.strawberry());
    this.producer.send(ProducerRecordTemplates.coconut());

    // act
    Tobby
        .replicator(ReplicatorConfig
            .builder()
            .dataSource(this.dataSource)
            .producer(this.mockProducer)
            .stopPredicate(it -> it.getWaveProcessed() == 0)
            .put(ReplicatorConfig.REPLICATORS_UPDATE_IDEMPOTENCE_TIME_TO_WAIT_BEFORE_REPLICATE, "PT0S")
            .build()
        )
        .replicate();

    // assert
    verify(this.mockProducer, times(2)).send(any());
  }

  @Test
  void mustReplicateDataToKafka() throws Exception {
    // arrange
    final var future = mock(Future.class);
    doReturn(RecordMetadataTemplates.timestampBasedRecordMetadata())
        .when(future)
        .get();
    doReturn(future)
        .when(this.mockProducer)
        .send(any());

    this.producer.send(ProducerRecordTemplates.strawberry());
    this.producer.send(ProducerRecordTemplates.coconut());

    // act
    this.replicate();

    // assert
    verify(this.mockProducer, times(2)).send(any());
  }

  @Test
  void mustNotProcessAlreadyAcquiredRecords() throws Exception {
    // arrange
    final var future = mock(Future.class);
    doReturn(RecordMetadataTemplates.timestampBasedRecordMetadata())
        .when(future)
        .get();
    doReturn(future)
        .when(this.mockProducer)
        .send(any());

    final var sent = this.producer.send(ProducerRecordTemplates.strawberry());
    this.producer.send(ProducerRecordTemplates.coconut());
    try (final var connection = this.dataSource.getConnection()) {
      this.tobby.recordDAO()
          .acquireDeleting(connection, sent.getId());
    }

    // act
    this.replicate();

    // assert
    verify(this.mockProducer, times(1)).send(any());
  }

  @Test
  void mustHaveNoTroublesWhenReplicateWithLockingAndHavingNoConcurrency() throws Exception {

    // arrange
    final var future = mock(Future.class);
    doReturn(RecordMetadataTemplates.timestampBasedRecordMetadata())
        .when(future)
        .get();
    doReturn(future)
        .when(this.mockProducer)
        .send(any());

    this.producer.send(ProducerRecordTemplates.strawberry());
    this.producer.send(ProducerRecordTemplates.coconut());

    // act
    final var acquiredLock = this.replicateLocking();

    // assert
    assertTrue(acquiredLock);
    verify(this.mockProducer, times(2)).send(any());

  }

  @Test
  void allThreadsMustHaveSuccessOnReplicatingWhenOneTreadEndsBeforeQueryTimeoutUsingLockingApproach() throws Exception {
    // arrange
    final var workers = 3;
    final var executorService = Threads.newPool(workers);

    final var future = mock(Future.class);
    doReturn(RecordMetadataTemplates.timestampBasedRecordMetadata())
        .when(future)
        .get();

    doReturn(future)
        .when(this.mockProducer)
        .send(any());

    this.producer.send(ProducerRecordTemplates.strawberry());
    this.producer.send(ProducerRecordTemplates.coconut());

    // act
    final var futures = new ArrayList<Future<Boolean>>();
    for (int i = 0; i < workers; i++) {
      final var theFuture =
          executorService.submit(() -> {
            return this.buildDefaultReplicator(Duration.ofMillis(150))
                .replicateLocking();
          });
      futures.add(theFuture);
    }

    final var replicationResult = futures
        .stream()
        .map(this::get)
        .sorted(Boolean::compare)
        .collect(Collectors.toList());

    // assert
    assertEquals(workers, replicationResult.size());
    assertTrue(replicationResult.contains(true));
    assertEquals("[true, true, true]", replicationResult.toString());
    verify(this.mockProducer, times(2)).send(any());
  }

  @Test
  void onlyOneThreadMustReplicateWithSuccessWhenUsingLockingApproach() throws Exception {

    // arrange
    final var workers = 3;
    final var executorService = Threads.newPool(workers);

    final Future<RecordMetadata> future = mock(Future.class);
    doReturn(RecordMetadataTemplates.timestampBasedRecordMetadata())
        .when(future)
        .get();

    doReturn(future)
        .when(this.mockProducer)
        .send(any());

    this.producer.send(ProducerRecordTemplates.strawberry());
    this.producer.send(ProducerRecordTemplates.coconut());

    // act
    final var futures = new ArrayList<Future<Boolean>>();
    for (int i = 0; i < workers; i++) {
      futures.add(executorService.submit(() -> this.replicateLocking(Duration.ofSeconds(8))));
    }

    final var replicationResult = futures
        .stream()
        .map(this::get)
        .sorted(Boolean::compare)
        .collect(Collectors.toList());

    // assert
    assertEquals(workers, replicationResult.size(), String.format("result was: %s", replicationResult));
    assertTrue(replicationResult.contains(true), String.format("result was: %s", replicationResult));
    assertEquals("[false, false, true]", replicationResult.toString());
    verify(this.mockProducer, times(2)).send(any());

  }

  @Test
  void mustReplicateDataToKafEvenWhenValueAndKeyAreNull() throws Exception {
    // arrange
    final var future = mock(Future.class);
    doReturn(RecordMetadataTemplates.timestampBasedRecordMetadata())
        .when(future)
        .get();
    doReturn(future)
        .when(this.mockProducer)
        .send(any());

    this.producer.send(ProducerRecordTemplates.banana());

    // act
    this.replicate();

    // assert
    verify(this.mockProducer, times(1)).send(any());
  }

  @SneakyThrows
  boolean get(Future<Boolean> it) {
    return it.get();
  }

  void replicate() {
    this.buildDefaultReplicator(DEFAULT_IDLE_TIMEOUT)
        .replicate();
  }

  boolean replicateLocking() {
    return this
        .buildDefaultReplicator()
        .replicateLocking();
  }

  boolean replicateLocking(Duration idleTimeout) {
    return this
        .buildDefaultReplicator(idleTimeout)
        .replicateLocking();
  }

  Replicators buildDefaultReplicator() {
    return this.buildDefaultReplicator(DEFAULT_IDLE_TIMEOUT);
  }

  Replicators buildDefaultReplicator(Duration idleTimeout) {
    return Tobby.replicator(ReplicatorConfig
        .builder()
        .dataSource(this.dataSource)
        .producer(this.mockProducer)
        .idleTimeout(idleTimeout)
        .put(ReplicatorConfig.REPLICATORS_UPDATE_IDEMPOTENCE_TIME_TO_WAIT_BEFORE_REPLICATE, "PT0S")
        .build()
    );
  }

}
