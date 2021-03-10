package com.mageddo.tobby.replicator;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;

import com.mageddo.tobby.ParameterDAO;
import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.UncheckedSQLException;

import lombok.extern.slf4j.Slf4j;

import static com.mageddo.tobby.Parameter.LAST_PROCESSED_TIMESTAMP;

@Slf4j
public class InsertIdempotenceBasedReplicator implements Replicator {

  private final BufferedReplicator bufferedReplicator;
  private final Connection readConn;
  private final Connection writeConn;
  private final RecordDAO recordDAO;
  private final ParameterDAO parameterDAO;
  private final Duration maxRecordDelayToCommit;
  private LocalDateTime lastRecordCreatedAt;

  public InsertIdempotenceBasedReplicator(BufferedReplicator bufferedReplicator, Connection readConn, Connection writeConn,
      RecordDAO recordDAO, ParameterDAO parameterDAO, Duration maxRecordDelayToCommit) {
    this.bufferedReplicator = bufferedReplicator;
    this.readConn = readConn;
    this.writeConn = writeConn;
    this.recordDAO = recordDAO;
    this.parameterDAO = parameterDAO;
    this.maxRecordDelayToCommit = maxRecordDelayToCommit;
  }

  @Override
  public void send(ProducedRecord record) {
    this.lastRecordCreatedAt = record.getCreatedAt();
    this.recordDAO.acquireInserting(this.writeConn, record.getId());
    this.bufferedReplicator.send(record);
  }

  @Override
  public void flush() {
    try {
      this.bufferedReplicator.flush();
      this.updateLastSent();
      this.writeConn.commit();
    } catch (SQLException e) {
      try {
        this.writeConn.rollback();
        throw new UncheckedSQLException(e);
      } catch (SQLException e2) {
        throw new UncheckedSQLException(e2);
      }
    }
  }

  @Override
  public void iterate() {
    this.recordDAO.iterateNotProcessedRecordsUsingInsertIdempotence(
        this.readConn, this::send, this.findLastUpdate(this.readConn)
    );
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
    this.parameterDAO.insertOrUpdate(connection, LAST_PROCESSED_TIMESTAMP, createdAt);
  }

  private LocalDateTime findLastUpdate(Connection connection) {
    return this.parameterDAO
        .findAsDateTime(
            connection, LAST_PROCESSED_TIMESTAMP, LocalDateTime.parse("2000-01-01T00:00:00")
        )
        .minusMinutes(this.maxRecordDelayToCommit.toMinutes());
  }
}
