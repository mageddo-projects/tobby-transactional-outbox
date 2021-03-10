package com.mageddo.tobby.replicator;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicInteger;

import com.mageddo.db.ConnectionUtils;
import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.RecordDAO;

public class DeleteIdempotenceBasedReplicator implements Replicator, StreamingIterator {

  private final RecordDAO recordDAO;
  private final Connection writeConn;
  private final Connection readConn;
  private final BufferedReplicator replicator;

  public DeleteIdempotenceBasedReplicator(Connection readConn, Connection writeConn, RecordDAO recordDAO,
      BufferedReplicator replicator) {
    this.recordDAO = recordDAO;
    this.writeConn = writeConn;
    this.readConn = readConn;
    this.replicator = replicator;
  }

  @Override
  public boolean send(ProducedRecord record) {
    this.recordDAO.acquireDeleting(this.writeConn, record.getId());
    if (this.replicator.send(record)) {
      this.flush();
    }
    return false;
  }

  @Override
  public void flush() {
    ConnectionUtils.useTransaction(this.writeConn, this.replicator::flush);
  }

  @Override
  public int iterate() {
    final AtomicInteger counter = new AtomicInteger();
    this.recordDAO.iterateNotProcessedRecordsUsingDeleteIdempotence(
        this.readConn,
        (record) -> {
          counter.incrementAndGet();
          this.send(record);
        }
    );
    this.flush();
    return counter.get();
  }
}
