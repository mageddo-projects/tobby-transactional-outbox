package com.mageddo.tobby.replicator;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import com.mageddo.tobby.ParameterDAO;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.UncheckedSQLException;
import com.mageddo.tobby.internal.utils.StopWatch;

import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.mageddo.tobby.Parameter.LAST_PROCESSED_TIMESTAMP;

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

  private final RecordDAO recordDAO;

  private final ParameterDAO parameterDAO;

  public KafkaReplicator(
      Producer<byte[], byte[]> producer, DataSource dataSource,
      RecordDAO recordDAO, ParameterDAO parameterDAO,
      Duration idleTimeout,
      Duration maxRecordDelayToCommit
  ) {
    this.recordDAO = recordDAO;
    this.parameterDAO = parameterDAO;
    this.producer = producer;
    this.dataSource = dataSource;
    this.idleTimeout = idleTimeout;
    this.maxRecordDelayToCommit = maxRecordDelayToCommit;
  }

  public void replicate() {
    final long millis = System.currentTimeMillis();
    log.info("status=replication-started");
    final AtomicReference<LocalDateTime> lastTimeProcessed = new AtomicReference<>(LocalDateTime.now());
    for (int wave = 1; this.shouldRun(lastTimeProcessed.get()); wave++) {
      this.processWave(lastTimeProcessed, wave);
    }
    log.info("status=replication-ended, duration={}", Duration.ofMillis(System.currentTimeMillis() - millis));
  }

  private void processWave(AtomicReference<LocalDateTime> lastTimeProcessed, int wave) {
    if (log.isDebugEnabled()) {
      log.debug("wave={}, status=loading-wave", wave);
    }
    try (Connection readConn = this.dataSource.getConnection(); Connection writeConn = this.dataSource.getConnection()) {
      final StopWatch stopWatch = StopWatch.createStarted();
      final BufferedReplicator sender = this.createSender(writeConn, wave);
      this.recordDAO.iterateNotProcessedRecordsUsingInsertIdempotence(readConn, (record) -> {
        sender.send(record);
        lastTimeProcessed.set(LocalDateTime.now());
      }, this.findLastUpdate(readConn));
      sender.flush();
      sender.updateLastSent();
      if (sender.size() > 0) {
        log.info(
            "wave={}, status=wave-ended, count={}, time={}, avg={}",
            wave, StopWatch.display(sender.size()), stopWatch.getDisplayTime(), stopWatch.getTime() / sender.size()
        );
      } else {
        if (log.isDebugEnabled()) {
          log.debug(
              "wave={}, status=wave-ended, count={}, time={}",
              wave, sender.size(), stopWatch.getDisplayTime()
          );
        }
      }
//      if(millisPassed(lastTimeProcessed.get()) / 1000 % NOTHING_TO_PROCESS_INTERVAL_SECONDS == 0){
//        log.info("status=nothing-to-process, idle={}", NOTHING_TO_PROCESS_INTERVAL_SECONDS);
//      }
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    }
  }

  private BufferedReplicator createSender(Connection writeConn, int wave) {
    return new BufferedReplicator(
        writeConn, this.recordDAO, this.parameterDAO, this.producer, wave
    );
  }

  private boolean shouldRun(LocalDateTime lastTimeProcessed) {
    return this.idleTimeout == Duration.ZERO
        || millisPassed(lastTimeProcessed) < this.idleTimeout.toMillis();
  }

  private long millisPassed(LocalDateTime lastTimeProcessed) {
    return ChronoUnit.MILLIS.between(lastTimeProcessed, LocalDateTime.now());
  }


  private LocalDateTime findLastUpdate(Connection connection) {
    return this.parameterDAO
        .findAsDateTime(
            connection, LAST_PROCESSED_TIMESTAMP, LocalDateTime.parse("2000-01-01T00:00:00")
        )
        .minusMinutes(this.maxRecordDelayToCommit.toMinutes());
  }
}
