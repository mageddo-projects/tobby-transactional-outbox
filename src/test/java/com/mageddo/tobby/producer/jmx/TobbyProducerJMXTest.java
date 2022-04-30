package com.mageddo.tobby.producer.jmx;

import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.dagger.TobbyFactory;
import com.mageddo.tobby.producer.ProducerConfig;

import com.mageddo.tobby.producer.ProducerJdbc;

import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import templates.ProducerRecordTemplates;
import testing.DBMigration;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@ExtendWith(MockitoExtension.class)
class TobbyProducerJMXTest {

  @Mock
  Producer producer;

  DataSource dataSource;

  TobbyFactory tobby;

  Connection connection;
  private RecordDAO recordDAO;
  private ProducerJdbc producerJdbc;


  @BeforeEach
  void before() throws SQLException {
    this.dataSource = DBMigration.migrateEmbeddedHSQLDB();
    this.connection = this.dataSource.getConnection();

    this.tobby = TobbyFactory.build(ProducerConfig
        .builder()
        .dataSource(this.dataSource)
        .producer(this.producer)
        .build());

    this.recordDAO = this.tobby.recordDAO();
    this.producerJdbc = this.tobby.jdbcProducer();

  }

  @AfterEach
  void after() throws SQLException {
    this.connection.close();
  }

  @Test
  void mustReSend(){
    // arrange
    final var record = ProducerRecordTemplates.banana();

    // act
    // assert
    final var produced = this.producerJdbc.send(record);
    assertEquals(1, this.recordDAO.findAll(this.connection).size());

    final var ids = String.format("%s|%s", produced.getId(), produced.getId());

    final var result = this.tobby.producerJMX()
        .reSend(ids);

    assertFalse(result.contains("Exception"), result);
    assertEquals(3, this.recordDAO.findAll(this.connection).size());
  }

}
