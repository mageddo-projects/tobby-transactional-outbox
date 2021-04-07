package com.mageddo.tobby.replicator;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import javax.sql.DataSource;

import com.mageddo.db.QueryTimeoutException;
import com.mageddo.tobby.Locker;
import com.mageddo.tobby.UncheckedSQLException;
import com.mageddo.tobby.internal.utils.StopWatch;

import lombok.extern.slf4j.Slf4j;

import static com.mageddo.tobby.internal.utils.StopWatch.display;

@Slf4j
public class Replicators {

  private final ReplicatorConfig config;
  private final IteratorFactory iteratorFactory;
  private final Locker locker;

  public Replicators(ReplicatorConfig config, IteratorFactory iteratorFactory, Locker locker) {
    this.config = config;
    this.iteratorFactory = iteratorFactory;
    this.locker = locker;
  }

  public void replicateLocking(){
    final DataSource dataSource = this.config.getDataSource();
    try(Connection conn = dataSource.getConnection()){
      this.locker.lock(conn);
      this.replicate();
    } catch (QueryTimeoutException e) {
      log.info("status=lostLocking");
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    }
  }

  public void replicate() {
    LocalDateTime lastTimeProcessed = LocalDateTime.now();
    int totalProcessed = 0;
    log.info("status=replication-started");
    for (int wave = 1; true; wave++) {
      final StopWatch stopWatch = StopWatch.createStarted();
      final int processed = this.processWave(wave);
      if (processed != 0) {
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
      if (!this.shouldRun(lastTimeProcessed)) {
        log.info(
            "status=idleTimedOut, lastTimeProcessed={}, idleTimeout={}", lastTimeProcessed, this.config.getIdleTimeout()
        );
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
        Connection readConn = this.getConnection();
        Connection writeConn = this.getConnection()
    ) {
      final boolean autoCommit = writeConn.getAutoCommit();
      if (autoCommit) {
        if (log.isDebugEnabled()) {
          log.info("status=disabling-auto-commit-for-this-transaction, wave={}", wave);
        }
        writeConn.setAutoCommit(false);
      }
      try {
        final StreamingIterator replicator = this.iteratorFactory.create(
            new BufferedReplicator(config.getProducer(), wave),
            readConn, writeConn, this.config
        );
        return replicator.iterate();
      } finally {
        if (autoCommit) {
          if (log.isDebugEnabled()) {
            log.info("status=enabling-auto-commit-back, wave={}", wave);
          }
          writeConn.setAutoCommit(true);
        }
      }
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    }
  }

  private Connection getConnection() throws SQLException {
    return this.config.getDataSource()
        .getConnection();
  }


  private boolean shouldRun(LocalDateTime lastTimeProcessed) {
    return this.config.getIdleTimeout() == Duration.ZERO
        || millisPassed(lastTimeProcessed) < this.config.getIdleTimeout().toMillis();
  }

  private long millisPassed(LocalDateTime lastTimeProcessed) {
    return ChronoUnit.MILLIS.between(lastTimeProcessed, LocalDateTime.now());
  }

}
