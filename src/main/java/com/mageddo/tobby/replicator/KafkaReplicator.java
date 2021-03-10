package com.mageddo.tobby.replicator;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import javax.sql.DataSource;

import com.mageddo.tobby.ParameterDAO;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.UncheckedSQLException;
import com.mageddo.tobby.internal.utils.StopWatch;

import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaReplicator {

  public static final int NOTHING_TO_PROCESS_INTERVAL_SECONDS = 10;
  private final Logger log = LoggerFactory.getLogger(getClass());

  /**
   * Producer used to send messages to kafka server.
   */
  private final Producer<byte[], byte[]> producer;

  /**
   * I think this variable objective is pretty clear, isn't?
   */
  private final DataSource dataSource;

  /**
   * How long stay waiting new records to be inserted at the database to be replicated,
   * if no record comes, stops the program.
   * <br><br>
   * Disabled by default setting value to {@link Duration#ZERO}.
   */
  private final Duration idleTimeout;

  /**
   * The amount of time Replicator will look back to find not replicated Records comparing the
   * DAT_CREATED of the last replicated Record.
   * <p>
   * This configuration is necessary because the date which is set to the record at the database,
   * is the date of the moment INSERT was executed, not committed, it means if that commit holds
   * e.g 10 minutes, then there is a big chance of more recent records be committed before,
   * it can occur for a matter of milliseconds though, no difference here.
   */
  private final Duration maxRecordDelayToCommit;

  private final IdempotenceStrategy idempotenceStrategy;

  private final RecordDAO recordDAO;

  private final ParameterDAO parameterDAO;

  public KafkaReplicator(
      Producer<byte[], byte[]> producer, DataSource dataSource,
      RecordDAO recordDAO, ParameterDAO parameterDAO,
      Duration idleTimeout,
      Duration maxRecordDelayToCommit,
      IdempotenceStrategy idempotenceStrategy) {
    this.recordDAO = recordDAO;
    this.parameterDAO = parameterDAO;
    this.producer = producer;
    this.dataSource = dataSource;
    this.idleTimeout = idleTimeout;
    this.maxRecordDelayToCommit = maxRecordDelayToCommit;
    this.idempotenceStrategy = idempotenceStrategy;
  }

  public void replicate() {
    log.info("status=replication-started");
    for (int wave = 1; true; wave++) {
      final StopWatch stopWatch = StopWatch.createStarted();
      final int processed = this.processWave(wave);
      if (stopWatch.getDuration()
          .toMillis() >= 1000) {
        log.info(
            "wave={}, status=wave-ended, count={}, time={}, avg={}",
            wave, StopWatch.display(processed), stopWatch.getDisplayTime(), stopWatch.getTime() / processed
        );
      } else {
        if (log.isDebugEnabled()) {
          log.debug(
              "wave={}, status=wave-ended, count={}, time={}",
              wave, processed, stopWatch.getDisplayTime()
          );
        }
      }
    }
//    log.info("status=replication-ended, duration={}", Duration.ofMillis(System.currentTimeMillis() - millis));
  }

  private int processWave(int wave) {
    if (log.isDebugEnabled()) {
      log.debug("wave={}, status=loading-wave", wave);
    }
    try (
        Connection readConn = this.dataSource.getConnection();
        Connection writeConn = this.dataSource.getConnection()
    ) {
      final StreamingIterator replicator = this.createReplicator(readConn, writeConn, wave);
      return replicator.iterate();
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    }
  }

  private StreamingIterator createReplicator(Connection readConn, Connection writeConn, int wave) {
    switch (this.idempotenceStrategy) {
      case INSERT:
        return new InsertIdempotenceBasedReplicator(
            readConn, writeConn, this.recordDAO, this.parameterDAO,
            new BufferedReplicator(this.producer, wave), this.maxRecordDelayToCommit
        );
      case DELETE:
        return new DeleteIdempotenceBasedReplicator(
            readConn, writeConn, this.recordDAO,
            new BufferedReplicator(this.producer, wave)
        );
      default:
        throw new IllegalArgumentException("Not strategy implemented for: " + this.idempotenceStrategy);
    }
  }

  private boolean shouldRun(LocalDateTime lastTimeProcessed) {
    return this.idleTimeout == Duration.ZERO
        || millisPassed(lastTimeProcessed) < this.idleTimeout.toMillis();
  }

  private long millisPassed(LocalDateTime lastTimeProcessed) {
    return ChronoUnit.MILLIS.between(lastTimeProcessed, LocalDateTime.now());
  }


}
