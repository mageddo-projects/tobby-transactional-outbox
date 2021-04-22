package com.mageddo.tobby.replicator;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicInteger;

import com.mageddo.db.ConnectionUtils;
import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.RecordProcessedDAO;

public class DeleteWithHistoryIdempotenceBasedReplicator implements Replicator, StreamingIterator {

  private final RecordDAO recordDAO;
  private final RecordProcessedDAO recordProcessedDAO;
  private final Connection writeConn;
  private final Connection readConn;
  private final BufferedReplicator replicator;
  private final int fetchSize;

  public DeleteWithHistoryIdempotenceBasedReplicator(
      Connection readConn, Connection writeConn, RecordDAO recordDAO,
      RecordProcessedDAO recordProcessedDAO, BufferedReplicator replicator, int fetchSize) {
    this.recordDAO = recordDAO;
    this.writeConn = writeConn;
    this.readConn = readConn;
    this.recordProcessedDAO = recordProcessedDAO;
    this.replicator = replicator;
    this.fetchSize = fetchSize;
  }

  @Override
  public boolean send(ProducedRecord record) {
    this.recordDAO.acquireDeleting(this.writeConn, record.getId());
    this.recordProcessedDAO.save(this.writeConn, record);
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
}
