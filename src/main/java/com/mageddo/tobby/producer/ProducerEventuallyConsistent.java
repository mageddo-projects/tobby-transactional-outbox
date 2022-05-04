package com.mageddo.tobby.producer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;

import javax.sql.DataSource;

import com.mageddo.db.ConnectionUtils;
import com.mageddo.tobby.ChangeAgents;
import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.ProducerRecord;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.UncheckedSQLException;
import com.mageddo.tobby.internal.utils.StopWatch;
import com.mageddo.tobby.internal.utils.Threads;

import org.apache.kafka.clients.producer.RecordMetadata;

import lombok.extern.slf4j.Slf4j;

import static com.mageddo.db.ConnectionUtils.runAndClose;
import static com.mageddo.tobby.producer.kafka.converter.ProducedRecordConverter.toKafkaProducerRecord;
import static com.mageddo.tobby.transaction.TransactionSynchronizationManager.registerSynchronization;

/**
 * Save the record to TTO_RECORD and returns to caller while tries to send to kafka in async if get succcess,
 * updates the TTO_RECORD to OK, if not, waits for the replicator do the JOB.
 */
@Slf4j
public class ProducerEventuallyConsistent implements Producer {

  private final org.apache.kafka.clients.producer.Producer<byte[], byte[]> kafkaProducer;
  private final RecordDAO recordDAO;
  private final DataSource dataSource;
  private final ExecutorService pool = Threads.newPool(20);

  public ProducerEventuallyConsistent(
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
      return ConnectionUtils.runAndClose(this.dataSource.getConnection(), (conn) -> {
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
  public ProducedRecord send(final Connection connection, final ProducerRecord record) {
    final StopWatch stopWatch = StopWatch.createStarted();
    final ProducedRecord producedRecord = this.recordDAO.save(connection, record);
    final long saveTime = stopWatch.getTime();
    stopWatch.split();
    registerSynchronization(() -> this.sendToKafka(producedRecord));
    if (log.isTraceEnabled()) {
      log.trace("status=sent, saveTime={}, sendTime={}, total={}",
          saveTime,
          stopWatch.getTime() - stopWatch.getSplitTime(),
          stopWatch.getTime()
      );
    }
    return producedRecord;
  }

  private void sendToKafka(ProducedRecord producedRecord) {
    final StopWatch stopWatch = StopWatch.createStarted();
    this.getKafkaProducer()
        .send(toKafkaProducerRecord(producedRecord), (metadata, e) -> {
          pool.submit(() -> {
            try {
              if (this.sentWithSuccess(metadata, e)) {
                final long kafkaSendTime = stopWatch.getTime();
                stopWatch.split();

                producedRecord
                    .setSentOffset(metadata.offset())
                    .setSentPartition(metadata.partition())
                ;
                this.markRecordAsSent(producedRecord);

                final long updateTime = stopWatch.getSplitTime();
                if (log.isDebugEnabled()) {
                  log.debug(
                      "status=updated, id={}, kafkaSendTime={}, updateTime={}, totalTime={}",
                      producedRecord.getId(), kafkaSendTime, updateTime, stopWatch.getTime()
                  );
                }
              } else {
                log.warn("status=cant-send-to-kafka id={} msg={}", producedRecord.getId(), e.getMessage(), e);
              }
            } catch (Exception e2) {
              log.warn("status=cant-update-record-status, id={}, msg={}", producedRecord.getId(), e2.getMessage(), e2);
            }
          });
        });
  }

  private boolean sentWithSuccess(RecordMetadata metadata, Exception e) {
    return e == null && metadata.partition() != RecordMetadata.UNKNOWN_PARTITION;
  }

  private org.apache.kafka.clients.producer.Producer<byte[], byte[]> getKafkaProducer() {
    return this.kafkaProducer;
  }

  private void markRecordAsSent(ProducedRecord producedRecord) {
    try {
      runAndClose(this.dataSource.getConnection(), (conn) -> {
        this.recordDAO.changeStatusToProcessed(conn, producedRecord, ChangeAgents.CALLBACK);
        return null;
      });
    } catch (SQLException e2) {
      throw new UncheckedSQLException(e2);
    }
  }
}
