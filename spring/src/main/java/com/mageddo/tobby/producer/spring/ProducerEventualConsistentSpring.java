package com.mageddo.tobby.producer.spring;

import java.sql.Connection;

import javax.sql.DataSource;

import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.ProducerRecord;
import com.mageddo.tobby.producer.Producer;
import com.mageddo.tobby.producer.ProducerEventualConsistent;
import com.mageddo.tobby.transaction.TransactionSynchronizationManager;

import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;

import lombok.extern.slf4j.Slf4j;

import static org.springframework.transaction.support.TransactionSynchronizationManager.isSynchronizationActive;
import static org.springframework.transaction.support.TransactionSynchronizationManager.registerSynchronization;

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
    final ProducedRecord r = this.send(this.getConnection(), record);
    if (isSynchronizationActive()) {
      registerSynchronization(new TransactionSynchronizationAdapter() {
        @Override
        public void afterCommit() {
          TransactionSynchronizationManager.execute();
        }
      });
    } else {
      TransactionSynchronizationManager.execute();
    }
    return r;
  }

  @Override
  public ProducedRecord send(Connection connection, ProducerRecord record) {
    return this.delegate.send(connection, record);
  }

  protected Connection getConnection() {
    return DataSourceUtils.getConnection(this.dataSource);
  }
}
