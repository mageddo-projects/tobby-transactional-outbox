package com.mageddo.tobby.producer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;

import javax.sql.DataSource;

import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.Tobby;
import com.mageddo.tobby.dagger.TobbyFactory;

import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import templates.ProducerRecordTemplates;
import testing.DBMigration;
import testing.PostgresExtension;

import static com.mageddo.tobby.Tobby.Config.TOBBY_RECORD_TABLE_NAME_PROP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith({PostgresExtension.class, MockitoExtension.class})
class ProducerJdbcCustomizedTableTest {

  TobbyFactory tobby;
  Producer jdbcProducer;
  Connection connection;
  DataSource dataSource;
  Jdbi jdbi;

  @BeforeEach
  void beforeEach() throws SQLException {
    this.dataSource = DBMigration.migrateEmbeddedPostgres();
    this.connection = this.dataSource.getConnection();
    this.tobby = TobbyFactory.build(
        ProducerConfig
            .builder()
            .producer(Mockito.mock(org.apache.kafka.clients.producer.Producer.class))
            .dataSource(this.dataSource)
            .build(),
        Tobby.Config
            .builder()
            .recordTableName("TTO_RECORD_V2")
            .build()
    );
    this.jdbcProducer = tobby.producer();
    this.jdbi = Jdbi.create(this.dataSource);
  }

  @AfterEach
  void afterEach() throws SQLException {
    this.connection.close();
    System.clearProperty(TOBBY_RECORD_TABLE_NAME_PROP);
  }

  @Test
  void mustSaveOnCustomizedRecordTable() {

    // arrange
    final var record = ProducerRecordTemplates.coconut();

    // act
    final var savedRecord = this.jdbcProducer.send(record);

    // assert
    assertNotNull(this.findRecord(savedRecord.getId()));
    assertEquals(0, this.countRecords("SELECT COUNT(1) FROM TTO_RECORD"));
    assertEquals(1, this.countRecords("SELECT COUNT(1) FROM TTO_RECORD_V2"));

  }

  private Integer countRecords(String sql) {
    return this.jdbi.withHandle(h -> {
          return h
              .createQuery(sql)
              .mapTo(int.class)
              .findOnly();
        }
    );
  }

  private ProducedRecord findRecord(UUID id) {
    return this.tobby.recordDAO()
        .find(this.connection, id);
  }

}
