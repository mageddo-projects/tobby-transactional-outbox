package com.mageddo.tobby.replicator.idempotencestrategy.batchdelete;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.sql.DataSource;

import com.mageddo.db.ConnectionUtils;
import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.internal.utils.BatchThread;
import com.mageddo.tobby.internal.utils.Threads;
import com.mageddo.tobby.replicator.BatchSender;
import com.mageddo.tobby.replicator.Replicator;
import com.mageddo.tobby.replicator.ReplicatorConfig;
import com.mageddo.tobby.replicator.StreamingIterator;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Singleton
public class BatchParallelDeleteIdempotenceBasedReplicator implements Replicator, StreamingIterator {

  private final List<ProducedRecord> buffer = new ArrayList<>();
  private final RecordDAO recordDAO;
  private final DataSource dataSource;
  private final BatchSender batchSender;
  private final ExecutorService pool;
  private final RecordDeleter recordDeleter;
  private final Config config;

  @Inject
  public BatchParallelDeleteIdempotenceBasedReplicator(
      RecordDAO recordDAO, DataSource dataSource, BatchSender batchSender,
      RecordDeleter recordDeleter, Config config
  ) {
    this.recordDAO = recordDAO;
    this.dataSource = dataSource;
    this.batchSender = batchSender;
    this.recordDeleter = recordDeleter;
    this.config = config;
    this.pool = Threads.newPool(config.getThreads());
  }

  @Override
  public boolean send(ProducedRecord record) {
    this.buffer.add(record);
    return this.buffer.size() >= this.config.getBufferSize();
  }

  @Override
  public void flush() {
    int from = 0;
    final BatchThread<Void> batchThread = new BatchThread<>();
    while (true) {
      final List<ProducedRecord> records = this.subList(from);
      if (records.isEmpty()) {
        break;
      }
      from += records.size();
      batchThread.add(() -> {
        try (Connection connection = this.dataSource.getConnection()) {
          ConnectionUtils.useTransaction(connection, () -> {
            this.recordDeleter.delete(connection, records, this.config.getDeleteMode());
            this.batchSender.send(records);
          });
          return null;
        }
      });
      Threads.executeAndGet(this.pool, batchThread.getCallables());
    }
  }

  @Override
  public int iterate(Connection readConn) {
    final AtomicInteger counter = new AtomicInteger();
    this.recordDAO.iterateOverRecords(
        readConn, this.config.getFetchSize(), (record) -> {
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
    @NonNull
    private int fetchSize;

    @NonNull
    private DeleteMode deleteMode;

    @NonNull
    private int bufferSize;

    @NonNull
    private int threadBufferSize;

    @NonNull
    private int threads;

    public static Config from(ReplicatorConfig config) {
      throw new UnsupportedOperationException();
    }
  }
}
