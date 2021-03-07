package com.mageddo.tobby.replicator;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import com.mageddo.tobby.ParameterDAO;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.UncheckedSQLException;

import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.mageddo.tobby.Parameter.LAST_PROCESSED_TIMESTAMP;
import static com.mageddo.tobby.producer.kafka.converter.ProducedRecordConverter.toKafkaProducerRecord;

public class KafkaReplicator {

  private final Logger log = LoggerFactory.getLogger(getClass());
  private final RecordDAO recordDAO;
  private final ParameterDAO parameterDAO;
  private final Producer<byte[], byte[]> producer;
  private final DataSource dataSource;
  private final Duration idleTimeout;

  public KafkaReplicator(
      Producer<byte[], byte[]> producer, DataSource dataSource,
      RecordDAO recordDAO, ParameterDAO parameterDAO,
      Duration idleTimeout
  ) {
    this.recordDAO = recordDAO;
    this.parameterDAO = parameterDAO;
    this.producer = producer;
    this.dataSource = dataSource;
    this.idleTimeout = idleTimeout;
  }

  public void replicate() {
    log.info("status=replication-started");
    final AtomicReference<LocalDateTime> lastTimeProcessed = new AtomicReference<>(LocalDateTime.now());
    for (int wave = 1; this.shouldRun(lastTimeProcessed.get()); wave++) {
      if (log.isDebugEnabled()) {
        log.debug("wave={}, status=loading-wave,", wave);
      }
      try (Connection connection = this.dataSource.getConnection()) {
        final boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        final int finalWave = wave;
        this.recordDAO.iterateNotProcessedRecords(connection, (record) -> {
          while (true) {
            try {
              this.producer
                  .send(toKafkaProducerRecord(record))
                  .get();
              this.updateLastUpdate(connection, record);
              if (log.isTraceEnabled()) {
                log.trace("wave={}, status=record-processed, id={}", finalWave, record.getId());
              }
              break;
            } catch (InterruptedException | ExecutionException e) {
              log.warn("wave={}, status=failed-to-post-to-kafka, msg={}", finalWave, e.getMessage(), e);
            } finally {
              lastTimeProcessed.set(LocalDateTime.now());
            }
          }
        }, this.findLastUpdate(connection));
        connection.setAutoCommit(true);
        connection.setAutoCommit(autoCommit);
        if (log.isDebugEnabled()) {
          log.debug("wave={}, status=wave-ended", wave);
        }
      } catch (SQLException e) {
        throw new UncheckedSQLException(e);
      }
    }
  }

  private boolean shouldRun(LocalDateTime lastTimeProcessed) {
    return this.idleTimeout == Duration.ZERO
        || ChronoUnit.MILLIS.between(lastTimeProcessed, LocalDateTime.now()) < this.idleTimeout.toMillis();
  }

  private void updateLastUpdate(Connection connection, com.mageddo.tobby.ProducedRecord record) {
    this.parameterDAO.insertOrUpdate(connection, LAST_PROCESSED_TIMESTAMP, record.getCreatedAt());
  }

  private LocalDateTime findLastUpdate(Connection connection) {
    return this.parameterDAO.findAsDateTime(
        connection, LAST_PROCESSED_TIMESTAMP, LocalDateTime.parse("2000-01-01T00:00:00")
    );
  }
}
