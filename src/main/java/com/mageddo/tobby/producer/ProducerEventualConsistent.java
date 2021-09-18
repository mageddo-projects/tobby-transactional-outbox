package com.mageddo.tobby.producer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import com.mageddo.db.ConnectionUtils;
import com.mageddo.tobby.ChangeAgents;
import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.ProducerRecord;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.UncheckedSQLException;
import com.mageddo.tobby.internal.utils.StopWatch;
import com.mageddo.tobby.internal.utils.Threads;

import lombok.extern.slf4j.Slf4j;

import static com.mageddo.db.ConnectionUtils.runAndClose;
import static com.mageddo.tobby.producer.kafka.converter.ProducedRecordConverter.toKafkaProducerRecord;
import static com.mageddo.tobby.transaction.TransactionSynchronizationManager.registerSynchronization;

@Slf4j
public class ProducerEventualConsistent implements Producer {

  private final org.apache.kafka.clients.producer.Producer<byte[], byte[]> kafkaProducer;
  private final RecordDAO recordDAO;
  private final DataSource dataSource;
  private final ExecutorService pool = Threads.newPool(20);

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

  private final AtomicInteger counter = new AtomicInteger();

  @Override
  public ProducedRecord send(final Connection connection, final ProducerRecord record) {
    final StopWatch stopWatch = StopWatch.createStarted();
    final ProducedRecord producedRecord = this.recordDAO.save(connection, record);
//    final ProducedRecord producedRecord = ProducedRecordConverter.from(UUID.randomUUID(), record);
    final long saveTime = stopWatch.getTime();
    stopWatch.split();
    registerSynchronization(() -> this.sendToKafka(producedRecord));

    log.trace("status=sent, saveTime={}, sendTime={}, total={}",
        saveTime,
        stopWatch.getTime() - stopWatch.getSplitTime(),
        stopWatch.getTime()
    );
    return producedRecord;
  }

  private void sendToKafka(ProducedRecord producedRecord) {
    final StopWatch stopWatch1 = StopWatch.createStarted();
    this.getKafkaProducer()
        .send(toKafkaProducerRecord(producedRecord), (metadata, e) -> {
          log.info("status=kafka-callback, id={}", producedRecord.getId());
          final long intervalTime = stopWatch1.getTime();
          pool.submit(() -> {
            try {
              log.info("status=pool-callback, id={}", producedRecord.getId());
              if (e == null) {
                final long interval2Time = stopWatch1.getTime();
                stopWatch1.split();
                this.markRecordAsSent(producedRecord);
                final long saveTime1 = stopWatch1.getSplitTime();
                log.info(
                    "status=updated, id={}, saveTime={}, interval={}, interval2={}, totalTime={}",
                    producedRecord.getId(), saveTime1, intervalTime, interval2Time,
                    stopWatch1.getTime()
                );
              } else {
                log.warn("status=cant-send-to-kafka id={} msg={}", producedRecord.getId(), e.getMessage(), e);
              }
            } catch (Exception e2){
              log.warn("status=cant-update-record-status, id={}, msg={}", producedRecord.getId(), e2.getMessage(), e2);
            }
          });
          log.info("status=callback, id={}, counter={}", producedRecord.getId(), counter.incrementAndGet());
        });
  }


  private org.apache.kafka.clients.producer.Producer<byte[], byte[]> getKafkaProducer() {
    return this.kafkaProducer;
  }

  private void markRecordAsSent(ProducedRecord producedRecord) {
    try {
      runAndClose(this.dataSource.getConnection(), (conn) -> {
        this.recordDAO.changeStatusToProcessed(conn, producedRecord.getId(), ChangeAgents.CALLBACK);
        return null;
      });
    } catch (SQLException e2) {
      throw new UncheckedSQLException(e2);
    }
  }
}
