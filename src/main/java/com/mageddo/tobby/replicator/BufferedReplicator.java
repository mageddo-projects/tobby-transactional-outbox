package com.mageddo.tobby.replicator;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.mageddo.tobby.ParameterDAO;
import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.UncheckedSQLException;
import com.mageddo.tobby.internal.utils.StopWatch;

import org.apache.kafka.clients.producer.Producer;

import lombok.extern.slf4j.Slf4j;

import static com.mageddo.tobby.Parameter.LAST_PROCESSED_TIMESTAMP;
import static com.mageddo.tobby.producer.kafka.converter.ProducedRecordConverter.toKafkaProducerRecord;

@Slf4j
public class BufferedReplicator implements Replicator {

  private LocalDateTime lastRecordCreatedAt;
  private final Connection connection;
  private final RecordDAO recordDAO;
  private final ParameterDAO parameterDAO;
  private final Producer<byte[], byte[]> producer;
  private final List<ProducedRecord> buffer;
  private final int bufferSize;
  private final int wave;

  public BufferedReplicator(
      Connection connection, RecordDAO recordDAO, ParameterDAO parameterDAO,
      Producer<byte[], byte[]> producer, int wave
  ) {
    this.connection = connection;
    this.recordDAO = recordDAO;
    this.parameterDAO = parameterDAO;
    this.producer = producer;
    this.wave = wave;
    this.bufferSize = 50_000;
    this.buffer = new ArrayList<>(this.bufferSize);
  }

  @Override
  public void send(ProducedRecord record) {
    //    final boolean autoCommit = connection.getAutoCommit();
//    connection.setAutoCommit(false);
//    connection.setAutoCommit(true);
//    connection.setAutoCommit(autoCommit);
    // FIXME criar um lote via codigo e commitar toda vez que esse lote atingir o limite,
    // fazer os updates e inserts em outra conexao para nao dar commit na que esta fazendo o select,
    // senao ela aborta o streaming
    this.recordDAO.acquireInserting(this.connection, record.getId());
    this.buffer.add(record);
    if (this.buffer.size() < this.bufferSize) {
      if (log.isTraceEnabled()) {
        log.trace("status=addToBuffer, id={}", record.getId());
      }
      return;
    }
    this.send();
  }

  @Override
  public void flush() {
    this.send();
  }

  public void send() {
    try {
      this.connection.commit();
      this.doSend();
      this.buffer.clear();
    } catch (SQLException e) {
      try {
        this.connection.rollback();
        throw new UncheckedSQLException(e);
      } catch (SQLException e2) {
        throw new UncheckedSQLException(e2);
      }
    }
  }

  private void doSend() {
    if (log.isDebugEnabled()) {
      log.debug("status=sending, wave={}, records={}", this.wave, this.buffer.size());
    }
    while (true) {
      try {
        final StopWatch recordStopWatch = StopWatch.createStarted();
        final List<RecordSend> futures = new ArrayList<>();
        for (ProducedRecord producedRecord : this.buffer) {
          futures.add(new RecordSend(
              producedRecord,
              this.producer.send(toKafkaProducerRecord(producedRecord))
          ));
        }
        if (log.isDebugEnabled()) {
          log.debug(
              "status=kafkaSendScheduled, wave={}, records={}, time={}",
              this.wave, this.buffer.size(), recordStopWatch.getTime()
          );
        }
        for (RecordSend future : futures) {
          future
              .getFuture()
              .get();
          this.lastRecordCreatedAt = future
              .getProducedRecord()
              .getCreatedAt();
        }
        final long produceTime = recordStopWatch.getSplitTime();
        recordStopWatch.split();
        if (log.isDebugEnabled()) {
          log.debug(
              "status=kafkaSendFlushed, wave={}, records={}, time={}",
              this.wave, this.buffer.size(), produceTime
          );
        }

        if (this.bufferSize > 1000) {
          log.info(
              "wave={}, quantity={}, status=kafkaSendFlushed, acquireTime={}, produceTime={}, recordsTime={}",
              this.wave,
              this.buffer.size(),
              StopWatch.display(-1),
              StopWatch.display(produceTime),
              recordStopWatch.getTime()
          );
        }
        break;
      } catch (InterruptedException | ExecutionException e) {
        log.warn("wave={}, status=failed-to-post-to-kafka, msg={}", this.wave, e.getMessage(), e);
      }
    }
  }

  public void updateLastSent() {
    this.updateLastUpdate(this.connection, this.lastRecordCreatedAt);
  }

  private void updateLastUpdate(Connection connection, LocalDateTime createdAt) {
    if (createdAt == null) {
      if (log.isDebugEnabled()) {
        log.debug("status=no-date-to-update");
      }
      return;
    }
    this.parameterDAO.insertOrUpdate(connection, LAST_PROCESSED_TIMESTAMP, createdAt);
  }

  public int size() {
    return this.buffer.size();
  }
}
