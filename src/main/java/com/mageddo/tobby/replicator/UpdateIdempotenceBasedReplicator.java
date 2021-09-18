package com.mageddo.tobby.replicator;

import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.sql.DataSource;

import com.mageddo.tobby.ChangeAgents;
import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.converter.ProducedRecordConverter;
import com.mageddo.tobby.internal.utils.BatchThread;
import com.mageddo.tobby.internal.utils.StopWatch;
import com.mageddo.tobby.internal.utils.Threads;

import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import static com.mageddo.db.ConnectionUtils.runAndClose;
import static com.mageddo.tobby.replicator.ReplicatorConfig.REPLICATORS_UPDATE_IDEMPOTENCE_BUFFER_SIZE;
import static com.mageddo.tobby.replicator.ReplicatorConfig.REPLICATORS_UPDATE_IDEMPOTENCE_THREADS;
import static com.mageddo.tobby.replicator.ReplicatorConfig.REPLICATORS_UPDATE_IDEMPOTENCE_THREAD_BUFFER_SIZE;
import static com.mageddo.tobby.replicator.ReplicatorConfig.REPLICATORS_UPDATE_IDEMPOTENCE_TIME_TO_WAIT_BEFORE_REPLICATE;

@Slf4j
@Singleton
public class UpdateIdempotenceBasedReplicator implements Replicator, StreamingIterator {

  private final List<ProducedRecord> buffer = new ArrayList<>();
  private final RecordDAO recordDAO;
  private final DataSource dataSource;
  private final BatchSender batchSender;
  private final ExecutorService pool;
  private final Config config;


  @Inject
  public UpdateIdempotenceBasedReplicator(RecordDAO recordDAO, DataSource dataSource, BatchSender batchSender,
      Config config) {
    this.recordDAO = recordDAO;
    this.dataSource = dataSource;
    this.batchSender = batchSender;
    this.pool = Threads.newPool(config.getThreads());
    this.config = config;
  }

  @Override
  public boolean send(ProducedRecord record) {
    this.buffer.add(record);
    final boolean exhausted = this.buffer.size() >= this.config.getBufferSize();
    if (exhausted) {
      this.flush();
    }
    return exhausted;
  }

  @Override
  public void flush() {
    if (this.buffer.isEmpty()) {
      if (log.isTraceEnabled()) {
        log.trace("status=nothingToFlush");
      }
      return;
    }
    final StopWatch flushTimer = StopWatch.createStarted();
    int from = 0;
    final BatchThread<Void> batchThread = new BatchThread<>();
    while (true) {
      final List<ProducedRecord> records = this.subList(from);
      if (records.isEmpty()) {
        break;
      }
      from += records.size();
      batchThread.add(() -> {
        return runAndClose(this.dataSource.getConnection(), (connection) -> {
          final StopWatch stopWatch = StopWatch.createStarted();
          this.recordDAO.changeStatusToProcessed(
              connection,
              ProducedRecordConverter.toIds(records),
              ChangeAgents.REPLICATOR
          );
          this.batchSender.send(records);
          if (log.isDebugEnabled()) {
            log.debug("status=replicated, records={}, time={}", records.size(), stopWatch.getDisplayTime());
          }
          return null;
        });
      });
    }
    Threads.executeAndGet(this.pool, batchThread.getCallables());
    if (log.isDebugEnabled()) {
      log.debug("status=flushed, records={}, time={}", this.buffer.size(), flushTimer.getDisplayTime());
    }
    this.buffer.clear();
  }

  @Override
  public int iterate(Connection readConn) {
    final AtomicInteger counter = new AtomicInteger();
    this.recordDAO.iterateOverRecordsInWaitingStatus(
        readConn,
        this.config.getFetchSize(),
        this.config.getTimeToWaitBeforeReplicate(),
        (record) -> {
          counter.incrementAndGet();
          this.send(record);
        }
    );
    this.flush();
    return counter.get();
  }


  private List<ProducedRecord> subList(int from) {
    return this.buffer.stream()
        .skip(from)
        .limit(this.config.getThreadBufferSize())
        .collect(Collectors.toList());
  }

  @Value
  @Builder
  public static class Config {

    /**
     * ResultSet buffer size when doing SELECT on TTO_RECORD
     */
    private int fetchSize;

    /**
     * How many records to store on memory before delete at the database and flush to kafka.
     */
    private int bufferSize;

    /**
     * How many records to send to one thread slicing from {@link #bufferSize}
     */
    private int threadBufferSize;

    /**
     * How many threads this replicator can use to execute {@link #flush()} in parallel.
     */
    private int threads;

    /**
     * How old the record must be before the replicator consider it to send to kafka
     */
    private Duration timeToWaitBeforeReplicate;

    public static Config from(ReplicatorConfig config) {
      return Config
          .builder()
          .fetchSize(config.getFetchSize())
          .bufferSize(config.getInt(REPLICATORS_UPDATE_IDEMPOTENCE_BUFFER_SIZE))
          .threads(config.getInt(REPLICATORS_UPDATE_IDEMPOTENCE_THREADS))
          .threadBufferSize(config.getInt(REPLICATORS_UPDATE_IDEMPOTENCE_THREAD_BUFFER_SIZE))
          .timeToWaitBeforeReplicate(
              Duration.parse(config.get(REPLICATORS_UPDATE_IDEMPOTENCE_TIME_TO_WAIT_BEFORE_REPLICATE))
          )
          .build();
    }

  }

}
