package com.mageddo.tobby.producer.spring;

import java.sql.Connection;

import javax.sql.DataSource;

import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.ProducerRecord;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.producer.Producer;

import lombok.extern.slf4j.Slf4j;

import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
public class ProducerSpring implements Producer {

  private final RecordDAO recordDAO;
  private final DataSource dataSource;

  public ProducerSpring(RecordDAO recordDAO, DataSource dataSource) {
    this.recordDAO = recordDAO;
    this.dataSource = dataSource;
  }

  @Override
  @Transactional
  public ProducedRecord send(ProducerRecord record) {
    return this.send(this.getConnection(), record);
  }

  @Override
  public ProducedRecord send(Connection connection, ProducerRecord record) {
    return this.recordDAO.save(connection, record);
  }

  protected Connection getConnection() {
    return DataSourceUtils.getConnection(this.dataSource);
  }
}
