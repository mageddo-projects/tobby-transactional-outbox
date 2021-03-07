package com.mageddo.tobby.producer.spring;

import java.sql.Connection;

import javax.sql.DataSource;

import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.ProducerRecord;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.RecordDAOUniversal;

import com.mageddo.tobby.producer.Producer;

import org.springframework.jdbc.datasource.DataSourceUtils;

public class ProducerSpring implements Producer {

  private final RecordDAO recordDAO;
  private final DataSource dataSource;

  public ProducerSpring(DataSource dataSource) {
    this(new RecordDAOUniversal(), dataSource);
  }

  public ProducerSpring(RecordDAO recordDAO, DataSource dataSource) {
    this.recordDAO = recordDAO;
    this.dataSource = dataSource;
  }

  @Override
  public ProducedRecord send(ProducerRecord record) {
    return this.send(DataSourceUtils.getConnection(this.dataSource), record);
  }

  @Override
  public ProducedRecord send(Connection connection, ProducerRecord record) {
    return this.recordDAO.save(connection, record);
  }
}
