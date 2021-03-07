package com.mageddo.tobby;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.mageddo.tobby.converter.HeadersConverter.encodeBase64;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static templates.ProducerRecordTemplates.strawberryWithHeaders;

abstract class RecordDAOTest {

  RecordDAO recordDAO;
  Connection connection;

  abstract DataSource dataSource();

  @BeforeEach
  void beforeEach() throws SQLException {
    final var tobby = TobbyConfig.build(this.dataSource());
    this.recordDAO = tobby.recordDAO();
    this.connection = dataSource().getConnection();
  }

  @AfterEach
  void afterEach() throws SQLException {
    this.connection.close();
  }

  @Test
  void mustSaveRecord() {
    // arrange
    final var record = strawberryWithHeaders();

    // act
    final var result = this.recordDAO.save(this.connection, record);

    // assert
    final var producedRecord = this.recordDAO.find(this.connection, result.getId());

    assertArrayEquals(record.getKey(), producedRecord.getKey());
    assertArrayEquals(record.getValue(), producedRecord.getValue());
    assertEquals(record.getPartition(), producedRecord.getPartition());
    assertEquals(record.getTopic(), producedRecord.getTopic());
    final var headers = producedRecord.getHeaders();
    assertEquals(encodeBase64(record.getHeaders()), encodeBase64(headers));
    assertEquals("1", new String(headers.getFirst("version").getValue()));
  }

}
