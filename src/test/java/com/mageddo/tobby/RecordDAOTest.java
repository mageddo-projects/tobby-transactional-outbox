package com.mageddo.tobby;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import com.mageddo.db.DuplicatedRecordException;
import com.mageddo.tobby.ProducedRecord.Status;
import com.mageddo.tobby.dagger.TobbyFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import static com.mageddo.tobby.converter.HeadersConverter.encodeBase64;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static templates.ProducerRecordTemplates.strawberryWithHeaders;

@Slf4j
abstract class RecordDAOTest {

  RecordDAO recordDAO;
  Connection connection;

  abstract DataSource dataSource();

  @BeforeEach
  void beforeEach() throws SQLException {
    final var tobby = TobbyFactory.build(this.dataSource());
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
    assertEquals(Status.WAIT, producedRecord.getStatus());
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
    this.recordDAO.iterateNotProcessedRecordsUsingInsertIdempotence(
        this.connection, 100, producedRecord -> {
          log.info("record={}", producedRecord);
          counter.incrementAndGet();
        },
        LocalDateTime.parse("2020-01-01T00:00:00")
    );

    // assert
    assertEquals(3, counter.get());
  }

  @Test
  void mustThrowDuplicatedRecordExceptionWhenAcquiresTwice() {
    // arrange
    final var id = UUID.fromString("89bb4b51-d9e3-4e0d-a4b0-c9186d5dc02e");

    // act
    this.recordDAO.acquireInserting(this.connection, id);
    assertThrows(DuplicatedRecordException.class, () -> {
      this.recordDAO.acquireInserting(this.connection, id);
    });

    // assert
  }

  void execute(String sql) {
    try (var stm = this.connection.prepareStatement(sql)) {
      stm.executeUpdate();
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    }
  }

  void shutdown() {

  }
}
