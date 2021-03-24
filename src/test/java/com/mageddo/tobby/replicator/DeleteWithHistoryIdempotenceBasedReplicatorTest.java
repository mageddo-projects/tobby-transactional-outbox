package com.mageddo.tobby.replicator;

import com.mageddo.tobby.Tobby;
import com.mageddo.tobby.TobbyConfig;

import org.junit.jupiter.api.Test;

import testing.DBMigration;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.spy;

class DeleteWithHistoryIdempotenceBasedReplicatorTest {

  private Replicator replicator;

  @Test
  void mustSendDeleteAndTrackRecordHistory(){

    // arrange
//    final var dataSource = DBMigration.migrateEmbeddedHSQLDB();
//    final var tobby = Tobby.build(dataSource);
//    tobby.replicator()
//    this.replicator = spy(this.tobby.replicator(this.mockProducer, Duration.ofMillis(600)));
//
    // act

    // assert

  }

}
