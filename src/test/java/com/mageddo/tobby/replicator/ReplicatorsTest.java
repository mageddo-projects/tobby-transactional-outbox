package com.mageddo.tobby.replicator;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import com.mageddo.tobby.TobbyConfig;

import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import lombok.SneakyThrows;
import templates.ProducerRecordTemplates;
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

  @Mock
  Producer<byte[], byte[]> mockProducer;
  com.mageddo.tobby.producer.Producer producer;
  TobbyConfig tobby;
  DataSource dataSource;

  @BeforeEach
  void beforeEach() {
    this.dataSource = DBMigration.migrateEmbeddedPostgres();
    this.tobby = TobbyConfig.build(this.dataSource);
    this.producer = this.tobby.producer();
  }

  @Test
  void mustReplicateDataToKafka() {
    // arrange
    doReturn(mock(Future.class)).when(this.mockProducer)
        .send(any());
    this.producer.send(ProducerRecordTemplates.strawberry());
    this.producer.send(ProducerRecordTemplates.coconut());

    // act
    this.buildDefaultReplicator()
        .replicate();

    // assert
    verify(this.mockProducer, times(2)).send(any());
  }

  @Test
  void mustNotProcessAlreadyAcquiredRecords() throws SQLException {
    // arrange
    doReturn(mock(Future.class))
        .when(this.mockProducer)
        .send(any());
    final var sent = this.producer.send(ProducerRecordTemplates.strawberry());
    this.producer.send(ProducerRecordTemplates.coconut());
    try (final var connection = this.dataSource.getConnection()) {
      this.tobby.recordDAO()
          .acquireDeleting(connection, sent.getId());
    }

    // act
    this.buildDefaultReplicator()
        .replicate();

    // assert
    verify(this.mockProducer, times(1)).send(any());
  }

  @Test
  void mustHaveNoTroublesWhenReplicateWithLockingAndHavingNoConcurrency() {

    // arrange
    doReturn(mock(Future.class))
        .when(this.mockProducer)
        .send(any())
    ;
    this.producer.send(ProducerRecordTemplates.strawberry());
    this.producer.send(ProducerRecordTemplates.coconut());

    // act
    final var acquiredLock = this.replicateLocking();

    // assert
    assertTrue(acquiredLock);
    verify(this.mockProducer, times(2)).send(any());

  }


  @Test
  void onlyOneThreadMustTryToReplicateWhenUsingLockingApproach() {

    // arrange
    final var workers = 3;
    final var executorService = Executors.newFixedThreadPool(workers);
    doReturn(mock(Future.class))
        .when(this.mockProducer)
        .send(any())
    ;
    this.producer.send(ProducerRecordTemplates.strawberry());
    this.producer.send(ProducerRecordTemplates.coconut());

    // act
    final var futures = new ArrayList<Future<Boolean>>();
    for (int i = 0; i < workers; i++) {
      final var future = executorService.submit(this::replicateLocking);
      futures.add(future);
    }

    final var replicationResult = futures
        .stream()
        .map(this::get)
        .sorted(Boolean::compare)
        .collect(Collectors.toList());

    // assert
    assertEquals(workers, replicationResult.size());
    assertTrue(replicationResult.contains(true));
    assertEquals("[false, false, true]", replicationResult.toString());
    verify(this.mockProducer, times(2)).send(any());

  }

  private boolean replicateLocking() {
    return this
        .buildDefaultReplicator()
        .replicateLocking();
  }

  @SneakyThrows
  private boolean get(Future<Boolean> it) {
    return it.get();
  }

  private Replicators buildDefaultReplicator() {
    return this.tobby.replicator(ReplicatorConfig
        .builder()
        .producer(this.mockProducer)
        .idleTimeout(Duration.ofMillis(600))
        .build()
    );
  }

}