package com.mageddo.tobby.producer;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.mageddo.tobby.ChangeAgents;
import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.ProducerRecord;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.UncheckedSQLException;
import com.mageddo.tobby.internal.utils.StopWatch;

import lombok.extern.slf4j.Slf4j;

import static com.mageddo.db.ConnectionUtils.useTransactionAndClose;
import static com.mageddo.tobby.producer.kafka.converter.ProducedRecordConverter.toKafkaProducerRecord;

@Slf4j
public class ProducerEventualConsistent implements Producer {

  private final org.apache.kafka.clients.producer.Producer<byte[], byte[]> kafkaProducer;
  private final RecordDAO recordDAO;
  private final DataSource dataSource;

  public ProducerEventualConsistent(
      org.apache.kafka.clients.producer.Producer<byte[], byte[]> kafkaProducer,
      RecordDAO recordDAO,
      DataSource dataSource) {
    this.kafkaProducer = kafkaProducer;
    this.recordDAO = recordDAO;
    this.dataSource = dataSource;
  }

  @Override
  public ProducedRecord send(ProducerRecord record) {
    final StopWatch totalStopWatch = StopWatch.createStarted();
    try {
      return useTransactionAndClose(this.dataSource.getConnection(), (conn) -> {
        return this.send(conn, record);
      });
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    } finally {
      if (log.isTraceEnabled()) {
        log.trace("status=committed, total={}", totalStopWatch.getTime());
      }
    }
  }

  @Override
  public ProducedRecord send(Connection connection, ProducerRecord record) {
    final ProducedRecord producedRecord = this.recordDAO.save(connection, record);
    this.getKafkaProducer().send(toKafkaProducerRecord(producedRecord), (metadata, e) -> {
      if (e == null) {
        this.markRecordAsSent(producedRecord);
      } else {
        log.warn("status=cant-send-to-kafka id={} msg={}", producedRecord.getId(), e.getMessage(), e);
      }
    });
    return producedRecord;
  }

  private org.apache.kafka.clients.producer.Producer<byte[], byte[]> getKafkaProducer() {
    return this.kafkaProducer;
  }

  private void markRecordAsSent(ProducedRecord producedRecord) {
    try {
      useTransactionAndClose(this.dataSource.getConnection(), (conn) -> {
        this.recordDAO.changeStatusToProcessed(conn, producedRecord.getId(), ChangeAgents.CALLBACK);
        return null;
      });
    } catch (SQLException e2) {
      throw new UncheckedSQLException(e2);
    }
  }
}
