package com.mageddo.tobby.producer;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.ProducerRecord;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.UncheckedSQLException;
import com.mageddo.tobby.internal.utils.StopWatch;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProducerJdbc implements Producer {

  private final RecordDAO recordDAO;
  private final DataSource dataSource;

  public ProducerJdbc(RecordDAO recordDAO, DataSource dataSource) {
    this.recordDAO = recordDAO;
    this.dataSource = dataSource;
  }

  @Override
  public ProducedRecord send(ProducerRecord record) {
    final StopWatch totalStopWatch = StopWatch.createStarted();
    final StopWatch commitStopWatch = new StopWatch();
    try (Connection connection = this.dataSource.getConnection();) {
      final boolean autoCommit = connection.getAutoCommit();
      final ProducedRecord r = this.recordDAO.save(connection, record);
      if (!autoCommit) {
        commitStopWatch.start();
        connection.commit();
        commitStopWatch.stop();
      }
      return r;
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    } finally {
      if (log.isTraceEnabled()) {
        log.trace("status=committed, commit={}, total={}", commitStopWatch.getTime(), totalStopWatch.getTime());
      }
    }
  }

  @Override
  public ProducedRecord send(Connection connection, ProducerRecord record) {
    return this.recordDAO.save(connection, record);
  }
}
