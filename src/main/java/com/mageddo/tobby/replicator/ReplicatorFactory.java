package com.mageddo.tobby.replicator;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import javax.sql.DataSource;

import com.mageddo.tobby.ParameterDAO;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.RecordProcessedDAO;
import com.mageddo.tobby.UncheckedSQLException;
import com.mageddo.tobby.internal.utils.StopWatch;

import org.apache.kafka.clients.producer.Producer;

import lombok.extern.slf4j.Slf4j;

import static com.mageddo.tobby.internal.utils.StopWatch.display;

@Slf4j
public class ReplicatorFactory {

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

  private final RecordProcessedDAO recordProcessedDAO;

  public ReplicatorFactory(
      Producer<byte[], byte[]> producer, DataSource dataSource,
      RecordDAO recordDAO, ParameterDAO parameterDAO,
      RecordProcessedDAO recordProcessedDAO, Duration idleTimeout,
      Duration maxRecordDelayToCommit,
      IdempotenceStrategy idempotenceStrategy
  ) {
    this.recordDAO = recordDAO;
    this.parameterDAO = parameterDAO;
    this.producer = producer;
    this.dataSource = dataSource;
    this.idleTimeout = idleTimeout;
    this.maxRecordDelayToCommit = maxRecordDelayToCommit;
    this.idempotenceStrategy = idempotenceStrategy;
    this.recordProcessedDAO = recordProcessedDAO;
  }

  public void replicate() {
    LocalDateTime lastTimeProcessed = LocalDateTime.now();
    int totalProcessed = 0;
    log.info("status=replication-started");
    for (int wave = 1; true; wave++) {
      final StopWatch stopWatch = StopWatch.createStarted();
      final int processed = this.processWave(wave);
      if(processed != 0){
        lastTimeProcessed = LocalDateTime.now();
      }
      totalProcessed += processed;
      if (stopWatch.getDuration()
          .toMillis() >= 1_000) {
        log.info(
            "wave={}, status=wave-ended, processed={}, time={}, avg={}, totalProcessed={}",
            display(wave), display(processed), stopWatch.getDisplayTime(),
            safeDivide(stopWatch, processed), display(totalProcessed)
        );
      } else {
        if (log.isDebugEnabled()) {
          log.debug(
              "wave={}, status=wave-ended, processed={}, time={}, totalProcessed={}",
              display(wave), display(processed), stopWatch.getDisplayTime(), display(totalProcessed)
          );
        }
        if (wave % 10_000 == 0) {
          log.info(
              "wave={}, status=waveReporting, totalProcessed={}",
              display(wave), display(totalProcessed)
          );
        }
      }
      if(!this.shouldRun(lastTimeProcessed)){
        log.info("status=idleTimedOut, lastTimeProcessed={}, idleTimeout={}", lastTimeProcessed, this.idleTimeout);
        return;
      }
    }
//    log.info("status=replication-ended, duration={}", Duration.ofMillis(System.currentTimeMillis() - millis));
  }

  private long safeDivide(StopWatch stopWatch, int processed) {
    if (processed == 0) {
      return 0;
    }
    return stopWatch.getTime() / processed;
  }

  private int processWave(int wave) {
    if (log.isDebugEnabled()) {
      log.debug("wave={}, status=loading-wave", wave);
    }
    try (
        Connection readConn = this.dataSource.getConnection();
        Connection writeConn = this.dataSource.getConnection()
    ) {
      final boolean autoCommit = writeConn.getAutoCommit();
      if(autoCommit){
        if(log.isDebugEnabled()){
          log.info("status=disabling-auto-commit-for-this-transaction, wave={}", wave);
        }
        writeConn.setAutoCommit(false);
      }
      try {
        final StreamingIterator replicator = this.createReplicator(readConn, writeConn, wave);
        return replicator.iterate();
      } finally {
        if(autoCommit){
          if(log.isDebugEnabled()){
            log.info("status=enabling-auto-commit-back, wave={}", wave);
          }
          writeConn.setAutoCommit(true);
        }
      }
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
      case DELETE_WITH_HISTORY:
        return new DeleteWithHistoryIdempotenceBasedReplicator(
            readConn, writeConn, this.recordDAO, this.recordProcessedDAO,
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
