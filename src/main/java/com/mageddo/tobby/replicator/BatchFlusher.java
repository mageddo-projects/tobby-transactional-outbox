package com.mageddo.tobby.replicator;

import com.mageddo.db.ConnectionUtils;
import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.internal.utils.BatchThread;
import com.mageddo.tobby.internal.utils.StopWatch;
import com.mageddo.tobby.internal.utils.Threads;

import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

@Slf4j
public class BatchFlusher {

  private final RecordDAO recordDAO;
  private final BatchSender batchSender;
  private final DataSource dataSource;
  private final List<ProducedRecord> buffer;
  private final ExecutorService pool;
  private long threadBufferSize;

  public BatchFlusher(
      RecordDAO recordDAO, BatchSender batchSender, DataSource dataSource,
      List<ProducedRecord> buffer, ExecutorService pool
  ) {
    this.recordDAO = recordDAO;
    this.batchSender = batchSender;
    this.dataSource = dataSource;
    this.buffer = buffer;
    this.pool = pool;
  }

  public void flush(){
    if (buffer.isEmpty()) {
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
        try (Connection connection = dataSource.getConnection()) {
          ConnectionUtils.useTransaction(connection, () -> {
            final StopWatch stopWatch = StopWatch.createStarted();
            this.recordDAO.changeStatus(connection, records, ProducedRecord.Status.DONE);
            this.batchSender.send(records);
            if (log.isDebugEnabled()) {
              log.debug("status=replicated, records={}, time={}", records.size(), stopWatch.getDisplayTime());
            }
          });
          return null;
        }
      });
    }
    Threads.executeAndGet(this.pool, batchThread.getCallables());
    if (log.isDebugEnabled()) {
      log.debug("status=flushed, records={}, time={}", this.buffer.size(), flushTimer.getDisplayTime());
    }
    buffer.clear();
  }

  private List<ProducedRecord> subList(int from) {
    return this.buffer.stream()
        .skip(from)
        .limit(this.threadBufferSize)
        .collect(Collectors.toList());
  }
}
