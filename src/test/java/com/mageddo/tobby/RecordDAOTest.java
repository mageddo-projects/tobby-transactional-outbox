package com.mageddo.tobby;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import static com.mageddo.tobby.converter.HeadersConverter.encodeBase64;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static templates.ProducerRecordTemplates.strawberryWithHeaders;

@Slf4j
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
    this.shutdown();
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
    assertEquals("1", new String(headers.getFirst("version")
        .getValue()));
  }

  @Test
  void mustIterateOverRecords() {
    // arrange
    final var counter = new AtomicInteger();
    this.recordDAO.save(this.connection, strawberryWithHeaders());
    this.recordDAO.save(this.connection, strawberryWithHeaders());
    this.recordDAO.save(this.connection, strawberryWithHeaders());

    // act
    this.recordDAO.iterateNotProcessedRecords(
        this.connection,
        producedRecord -> {
          log.info("record={}", producedRecord);
          counter.incrementAndGet();
        },
        LocalDateTime.now().minusDays(1)
    );

    // assert
    assertEquals(3, counter.get());
  }

  void execute(String sql) {
    try (var stm = this.connection.prepareStatement(sql)) {
      stm.executeUpdate();
    } catch (SQLException e){
      throw new UncheckedSQLException(e);
    }
  }

  void shutdown(){

  }
}
