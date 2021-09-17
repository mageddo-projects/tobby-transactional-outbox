package com.mageddo.tobby.producer.spring;

import java.sql.Connection;

import javax.sql.DataSource;

import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.ProducerRecord;
import com.mageddo.tobby.producer.Producer;
import com.mageddo.tobby.producer.ProducerEventualConsistent;

import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.annotation.Transactional;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProducerEventualConsistentSpring implements Producer {

  private final DataSource dataSource;
  private final ProducerEventualConsistent delegate;

  public ProducerEventualConsistentSpring(DataSource dataSource, ProducerEventualConsistent delegate) {
    this.dataSource = dataSource;
    this.delegate = delegate;
  }

  @Override
  @Transactional
  public ProducedRecord send(ProducerRecord record) {
    return this.send(this.getConnection(), record);
  }

  @Override
  public ProducedRecord send(Connection connection, ProducerRecord record) {
    return this.delegate.send(connection, record);
  }

  protected Connection getConnection() {
    return DataSourceUtils.getConnection(this.dataSource);
  }
}
