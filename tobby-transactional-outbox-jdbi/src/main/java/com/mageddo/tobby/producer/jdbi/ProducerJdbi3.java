package com.mageddo.tobby.producer.jdbi;

import java.sql.Connection;

import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.ProducerRecord;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.RecordDAOHsqldb;

import com.mageddo.tobby.producer.Producer;

import org.jdbi.v3.core.Jdbi;

public class ProducerJdbi3 implements Producer {

  private final RecordDAO recordDAO;
  private final Jdbi jdbi;

  public ProducerJdbi3(Jdbi jdbi) {
    this(new RecordDAOHsqldb(), jdbi);
  }

  public ProducerJdbi3(RecordDAO recordDAO, Jdbi jdbi) {
    this.recordDAO = recordDAO;
    this.jdbi = jdbi;
  }

  @Override
  public ProducedRecord send(ProducerRecord record) {
    return this.jdbi.withHandle(h -> this.recordDAO.save(h.getConnection(), record));
  }

  @Override
  public ProducedRecord send(Connection connection, ProducerRecord record) {
    return this.recordDAO.save(connection, record);
  }
}
