package com.mageddo.tobby.producer;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.ProducerRecord;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.RecordDAOHsqldb;
import com.mageddo.tobby.UncheckedSQLException;

public class ProducerJdbc implements Producer {

  private final RecordDAO recordDAO;
  private final DataSource dataSource;

  public ProducerJdbc(DataSource dataSource) {
    this(new RecordDAOHsqldb(), dataSource);
  }

  public ProducerJdbc(RecordDAO recordDAO, DataSource dataSource) {
    this.recordDAO = recordDAO;
    this.dataSource = dataSource;
  }

  @Override
  public ProducedRecord send(ProducerRecord record) {
    try (Connection connection = this.dataSource.getConnection();) {
      final boolean autoCommit = connection.getAutoCommit();
      connection.setAutoCommit(false);
      final ProducedRecord r = this.recordDAO.save(connection, record);
      connection.setAutoCommit(true);
      connection.setAutoCommit(autoCommit);
      return r;
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    }
  }

  @Override
  public ProducedRecord send(Connection connection, ProducerRecord record) {
    return this.recordDAO.save(connection, record);
  }
}
