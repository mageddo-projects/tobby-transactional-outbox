package com.mageddo.tobby.producer;

import com.mageddo.db.DB;
import com.mageddo.tobby.ProducedRecord.Status;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.factory.DAOFactory;
import com.mageddo.tobby.internal.utils.Threads;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import templates.ProducerRecordTemplates;
import templates.RecordMetadataTemplates;
import testing.DBMigration;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
class ProducerEventuallyConsistentTest {

  @Captor
  ArgumentCaptor<Callback> captor;

  @Mock
  Producer producer;

  DataSource dataSource;

  ProducerEventuallyConsistent tobbyProducer;

  RecordDAO recordDao;

  Connection connection;

  @BeforeEach
  void before() throws Exception {
    this.dataSource = DBMigration.migrateEmbeddedHSQLDB();
    this.connection = this.dataSource.getConnection();
    this.recordDao = DAOFactory.createRecordCustomTableDao(DB.POSTGRESQL);
    this.tobbyProducer = new ProducerEventuallyConsistent(
        this.producer,
        this.recordDao,
        this.dataSource
    );
  }

  @AfterEach
  void after() throws SQLException {
    this.connection.close();
  }

  @Test
  void mustSendAndUpdateRecordToProcessed() throws Exception {
    // arrange
    final var record = ProducerRecordTemplates.coconut();

    doReturn(mock(Future.class))
        .when(this.producer)
        .send(any(), this.captor.capture());

    // act
    final var producedRecord = this.tobbyProducer.send(record);

    // assert
    {
      final var found = this.recordDao.find(this.connection, producedRecord.getId());
      assertNotNull(found);
      assertEquals(Status.WAIT, found.getStatus());
      assertNull(found.getSentPartition());
      assertNull(found.getSentOffset());
    }

    this.captor
        .getValue()
        .onCompletion(RecordMetadataTemplates.timestampBasedRecordMetadata(), null);
    Threads.sleep(Duration.ofSeconds(1));

    {
      final var found = this.recordDao.find(this.connection, producedRecord.getId());
      assertNotNull(found);
      assertEquals(Status.OK, found.getStatus());
      assertNotNull(found.getSentPartition());
      assertNotNull(found.getSentOffset());
    }

  }

}
