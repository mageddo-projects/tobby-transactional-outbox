package com.mageddo.tobby.replicator;

import java.sql.Connection;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicInteger;

import com.mageddo.db.ConnectionUtils;
import com.mageddo.tobby.ParameterDAO;
import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.RecordDAO;

import lombok.extern.slf4j.Slf4j;

import static com.mageddo.tobby.Parameter.LAST_PROCESSED_TIMESTAMP;

@Slf4j
public class InsertIdempotenceBasedReplicator implements Replicator, StreamingIterator {

  private final BufferedReplicator bufferedReplicator;
  private final Connection readConn;
  private final Connection writeConn;
  private final RecordDAO recordDAO;
  private final ParameterDAO parameterDAO;
  private final Duration maxRecordDelayToCommit;
  private final int fetchSize;
  private LocalDateTime lastRecordCreatedAt;

  public InsertIdempotenceBasedReplicator(
      Connection readConn, Connection writeConn, RecordDAO recordDAO,
      ParameterDAO parameterDAO, BufferedReplicator bufferedReplicator,
      Duration maxRecordDelayToCommit, int fetchSize
  ) {
    this.bufferedReplicator = bufferedReplicator;
    this.readConn = readConn;
    this.writeConn = writeConn;
    this.recordDAO = recordDAO;
    this.parameterDAO = parameterDAO;
    this.maxRecordDelayToCommit = maxRecordDelayToCommit;
    this.fetchSize = fetchSize;
  }

  @Override
  public boolean send(ProducedRecord record) {
    this.lastRecordCreatedAt = record.getCreatedAt();
    this.recordDAO.acquireInserting(this.writeConn, record.getId());
    if (this.bufferedReplicator.send(record)) {
      this.flush();
    }
    return false;
  }

  @Override
  public void flush() {
    ConnectionUtils.useTransaction(this.writeConn, () -> {
      this.bufferedReplicator.flush();
      this.updateLastSent();
    });
  }

  @Override
  public int iterate(Connection readConn) {
    final AtomicInteger counter = new AtomicInteger();
    this.recordDAO.iterateNotProcessedRecordsUsingInsertIdempotence(
        this.readConn, this.fetchSize, (record) -> {
          counter.incrementAndGet();
          this.send(record);
        },
        this.findLastUpdate(this.readConn)
    );
    this.flush();
    return counter.get();
  }

  private void updateLastSent() {
    this.updateLastUpdate(this.writeConn, this.lastRecordCreatedAt);
  }

  private void updateLastUpdate(Connection connection, LocalDateTime createdAt) {
    if (createdAt == null) {
      if (log.isDebugEnabled()) {
        log.debug("status=no-date-to-update");
      }
      return;
    }
    this.parameterDAO.insertOrUpdate(connection, LAST_PROCESSED_TIMESTAMP, createdAt.toString());
  }

  private LocalDateTime findLastUpdate(Connection connection) {
    return this.parameterDAO
        .findAsDateTime(
            connection, LAST_PROCESSED_TIMESTAMP, LocalDateTime.parse("2000-01-01T00:00:00")
        )
        .minusMinutes(this.maxRecordDelayToCommit.toMinutes());
  }
}
