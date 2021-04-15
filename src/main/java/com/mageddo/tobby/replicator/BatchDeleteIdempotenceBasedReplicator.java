package com.mageddo.tobby.replicator;

import java.sql.Connection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.mageddo.db.ConnectionUtils;
import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.RecordDAO;

public class BatchDeleteIdempotenceBasedReplicator implements Replicator, StreamingIterator {

  private final RecordDAO recordDAO;
  private final Connection writeConn;
  private final Connection readConn;
  private final BufferedReplicator replicator;
  private final int fetchSize;

  public BatchDeleteIdempotenceBasedReplicator(
      Connection readConn, Connection writeConn, RecordDAO recordDAO,
      BufferedReplicator replicator, int fetchSize
  ) {
    this.recordDAO = recordDAO;
    this.writeConn = writeConn;
    this.readConn = readConn;
    this.replicator = replicator;
    this.fetchSize = fetchSize;
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
      this.recordDAO.acquireDeleting(
          this.writeConn,
          this.replicator.getBuffer()
              .stream()
              .map(ProducedRecord::getId)
              .collect(Collectors.toList())
      );
      this.replicator.flush();
    });
  }

  @Override
  public int iterate() {
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
}
