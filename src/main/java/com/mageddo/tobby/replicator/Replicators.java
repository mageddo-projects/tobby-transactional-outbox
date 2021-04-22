package com.mageddo.tobby.replicator;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import javax.sql.DataSource;

import com.mageddo.db.ConnectionUtils;
import com.mageddo.db.QueryTimeoutException;
import com.mageddo.tobby.Locker;
import com.mageddo.tobby.UncheckedSQLException;
import com.mageddo.tobby.internal.utils.StopWatch;

import lombok.extern.slf4j.Slf4j;

import static com.mageddo.db.ConnectionUtils.useTransaction;
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

  /**
   * Replicate records to Kafka making sure only one thread will execute at time by using RDMS resource locking
   *
   * @return true or false about acquiring the lock
   */
  public boolean replicateLocking() {
    log.info("status=replicateLocking");
    final DataSource dataSource = this.config.getDataSource();
    try (Connection conn = dataSource.getConnection()) {
      useTransaction(conn, () -> {
        this.locker.lock(conn);
        this.replicate(conn);
      });
      return true;
    } catch (QueryTimeoutException e) {
      log.info("status=lostLocking");
      return false;
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    }
  }

  public void replicate() {
    this.replicate(null);
  }

  void replicate(Connection readConn) {
    LocalDateTime lastTimeProcessed = LocalDateTime.now();
    int totalProcessed = 0;
    log.info("status=replication-started");
    for (int wave = 1; true; wave++) {
      final StopWatch stopWatch = StopWatch.createStarted();

      final int processed = this.processWave(wave, readConn);
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

      final ReplicatorContextVars contextVars = ReplicatorContextVars
          .builder()
          .waveDuration(stopWatch.getDuration())
          .waveProcessed(processed)
          .durationSinceLastTimeProcessed(Duration.ofMillis(millisPassed(lastTimeProcessed)))
          .wave(wave)
          .build();
      if (this.shouldStop(contextVars)) {
        log.info("status=replicatorIsConsideredIdle, action=exiting, contextVars={}", contextVars);
        return;
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

  private boolean shouldStop(ReplicatorContextVars contextVars) {
    return config.getStopPredicate()
        .test(contextVars);
  }

  private long safeDivide(StopWatch stopWatch, int processed) {
    if (processed == 0) {
      return 0;
    }
    return stopWatch.getTime() / processed;
  }

  private int processWave(int wave, Connection readConnParam) {
    if (log.isDebugEnabled()) {
      log.debug("wave={}, status=loading-wave", wave);
    }
    final Connection readConn = this.chooseReadConnection(readConnParam);
    try (Connection writeConn = this.getConnection()) {
      return useTransaction(writeConn, () -> {
        final BufferedReplicator bufferedReplicator = new BufferedReplicator(
            this.config.getProducer(), this.config.getBufferSize(), wave
        );
        final StreamingIterator replicator = this.iteratorFactory.create(
            bufferedReplicator, readConn, writeConn, this.config
        );
        return replicator.iterate(readConn);
      });
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    } finally {
      if (readConnParam == null) {
        ConnectionUtils.quietClose(readConn);
      }
    }
  }

  private Connection chooseReadConnection(Connection readConnParam) {
    if (readConnParam != null) {
      return readConnParam;
    }
    return this.getConnection();
  }

  private Connection getConnection() {
    try {
      return this.config
          .getDataSource()
          .getConnection();
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    }
  }


  private boolean shouldRun(LocalDateTime lastTimeProcessed) {
    return this.config.getIdleTimeout() == Duration.ZERO
        || millisPassed(lastTimeProcessed) < this.config.getIdleTimeout()
        .toMillis();
  }

  private long millisPassed(LocalDateTime lastTimeProcessed) {
    return ChronoUnit.MILLIS.between(lastTimeProcessed, LocalDateTime.now());
  }

}
