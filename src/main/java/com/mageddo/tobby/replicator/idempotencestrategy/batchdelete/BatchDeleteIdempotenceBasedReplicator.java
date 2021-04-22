package com.mageddo.tobby.replicator.idempotencestrategy.batchdelete;

import java.sql.Connection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.mageddo.db.ConnectionUtils;
import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.replicator.BufferedReplicator;
import com.mageddo.tobby.replicator.Replicator;
import com.mageddo.tobby.replicator.StreamingIterator;

public class BatchDeleteIdempotenceBasedReplicator implements Replicator, StreamingIterator {

  private final RecordDAO recordDAO;
  private final Connection writeConn;
  private final Connection readConn;
  private final BufferedReplicator replicator;
  private final int fetchSize;
  private final DeleteMode deleteMode;

  public BatchDeleteIdempotenceBasedReplicator(
      Connection readConn, Connection writeConn, RecordDAO recordDAO,
      BufferedReplicator replicator, int fetchSize,
      BatchDeleteIdempotenceStrategyConfig config
  ) {
    this.recordDAO = recordDAO;
    this.writeConn = writeConn;
    this.readConn = readConn;
    this.replicator = replicator;
    this.fetchSize = fetchSize;
    this.deleteMode = config.getDeleteMode();
  }

  @Override
  public boolean send(ProducedRecord record) {
    if (this.replicator.send(record)) {
      this.flush();
    }
    return false;
  }

  @Override
  public void flush() {
    ConnectionUtils.useTransaction(this.writeConn, () -> {
      this.batchDelete();
      this.replicator.flush();
    });
  }

  @Override
  public int iterate(Connection readConn) {
    final AtomicInteger counter = new AtomicInteger();
    this.recordDAO.iterateOverRecords(
        this.readConn, this.fetchSize, (record) -> {
          counter.incrementAndGet();
          this.send(record);
        }
    );
    this.flush();
    return counter.get();
  }

  private void batchDelete() {
    final List<UUID> recordIds = this.replicator.getBuffer()
        .stream()
        .map(ProducedRecord::getId)
        .collect(Collectors.toList());
    switch (this.deleteMode) {
      case BATCH_DELETE:
        this.recordDAO.acquireDeletingUsingBatch(this.writeConn, recordIds);
        break;
      case BATCH_DELETE_USING_IN:
        this.recordDAO.acquireDeletingUsingIn(this.writeConn, recordIds);
        break;
      case BATCH_DELETE_USING_THREADS:
        this.recordDAO.acquireDeletingUsingThreads(this.writeConn, recordIds);
        break;
    }
  }

}
