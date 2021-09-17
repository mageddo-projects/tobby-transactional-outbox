package com.mageddo.tobby;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.mageddo.db.DB;
import com.mageddo.db.DuplicatedRecordException;
import com.mageddo.tobby.converter.HeadersConverter;
import com.mageddo.tobby.converter.ProducedRecordConverter;
import com.mageddo.tobby.internal.utils.Base64;
import com.mageddo.tobby.internal.utils.StopWatch;
import com.mageddo.tobby.internal.utils.Validator;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RecordDAOGeneric implements RecordDAO {

  private final DB db;
  private final ExecutorService pool;

  public RecordDAOGeneric(DB db, ExecutorService pool) {
    this.db = db;
    this.pool = pool;
  }

  @Override
  public ProducedRecord save(Connection connection, ProducerRecord record) {
    final StopWatch stopWatch = StopWatch.createStarted();
    final StringBuilder sql = new StringBuilder()
        .append("INSERT INTO TTO_RECORD ( \n")
        .append("  IDT_TTO_RECORD, NAM_TOPIC, NUM_PARTITION, \n")
        .append("  TXT_KEY, TXT_VALUE, TXT_HEADERS \n")
        .append(") VALUES ( \n")
        .append("  ?, ?, ?, \n")
        .append("  ?, ?, ? \n")
        .append(") \n");
    try (final PreparedStatement stm = connection.prepareStatement(sql.toString())) {
      final UUID id = fillStatement(record, stm);
      stm.executeUpdate();
      return ProducedRecordConverter.from(id, record);
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    } finally {
      if (log.isTraceEnabled()) {
        log.trace("status=save, statementTime={}", stopWatch.getTime());
      }
    }
  }

  @Override
  public ProducedRecord find(Connection connection, UUID id) {
    final String sql = "SELECT * FROM TTO_RECORD WHERE IDT_TTO_RECORD = ?";
    try (PreparedStatement stm = connection.prepareStatement(sql)) {
      stm.setString(1, id.toString());
      try (ResultSet rs = stm.executeQuery()) {
        if (rs.next()) {
          return ProducedRecordConverter.map(rs);
        }
        return null;
      }
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    }
  }

  @Override
  public void iterateNotProcessedRecordsUsingInsertIdempotence(
      Connection connection, int fetchSize, Consumer<ProducedRecord> consumer, LocalDateTime from
  ) {
    final String sql = new StringBuilder()
        .append("SELECT * FROM TTO_RECORD R \n")
        .append("WHERE DAT_CREATED > ? \n")
        .append("AND DAT_CREATED < ? \n")
        .append("AND NOT EXISTS ( \n")
        .append("  SELECT 1 FROM TTO_RECORD_PROCESSED \n")
        .append("  WHERE IDT_TTO_RECORD = R.IDT_TTO_RECORD \n")
        .append("  AND DAT_CREATED > ? \n")
        .append("  AND DAT_CREATED < ? \n")
        .append(") \n")
        .append("ORDER BY DAT_CREATED ASC \n")
        .toString();
    final StopWatch stopWatch = StopWatch.createStarted();
    try (PreparedStatement stm = this.createStreamingStatement(connection, sql, fetchSize)) {
      // prevent scanning too many future partitions
      final LocalDateTime to = LocalDateTime.now()
          .plusDays(2);
      final Timestamp toTimestamp = Timestamp.valueOf(to);
      stm.setTimestamp(1, Timestamp.valueOf(from));
      stm.setTimestamp(2, toTimestamp);
      stm.setTimestamp(3, Timestamp.valueOf(from));
      stm.setTimestamp(4, toTimestamp);
      try (ResultSet rs = stm.executeQuery()) {
//        if(log.isDebugEnabled()){
        log.info("status=queryExecuted, time={}, from={}, to={}", stopWatch.getDisplayTime(), from, to);
//        }
        while (rs.next()) {
          consumer.accept(ProducedRecordConverter.map(rs));
        }
      }
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    }
  }

  // FIXME FAZER SAVEPOINT PORQUE TEM BANCOS QUE FAZEM ROLLBACK QUANDO TOMAM ERROR, e.g Postgres
  @Override
  public void acquireInserting(Connection connection, UUID id) {
    final String sql = "INSERT INTO TTO_RECORD_PROCESSED (IDT_TTO_RECORD) VALUES (?)";
    try (PreparedStatement stm = connection.prepareStatement(sql)) {
      stm.setString(1, String.valueOf(id));
      stm.executeUpdate();
    } catch (SQLException e) {
      throw DuplicatedRecordException.check(this.db, id, e);
//      if (e.getMessage()
//          .toUpperCase()
//          .contains("TTO_RECORD_PROCESSED_PK")) {
//        throw new DuplicatedRecordException(id, e);
//      }
//      throw new UncheckedSQLException(e);
    }
  }

  @Override
  public void iterateOverRecords(Connection connection, int fetchSize, Consumer<ProducedRecord> consumer) {
    final StopWatch stopWatch = StopWatch.createStarted();
    try (PreparedStatement stm = this.createStreamingStatement(connection, "SELECT * FROM TTO_RECORD", fetchSize)) {
      try (ResultSet rs = stm.executeQuery()) {
        if (log.isDebugEnabled()) {
          log.debug("status=queryExecuted, time={}", stopWatch.getDisplayTime());
        }
        while (rs.next()) {
          consumer.accept(ProducedRecordConverter.map(rs));
        }
      }
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    }
  }

  @Override
  public void acquireDeletingUsingThreads(Connection connection, List<UUID> recordIds) {
    if (recordIds.isEmpty()) {
      if (log.isTraceEnabled()) {
        log.trace("m=acquireDeletingUsingThreads, status=noRecordsToDelete");
      }
      return;
    }
    final StopWatch stopWatch = StopWatch.createStarted();
    final List<Future> promises = recordIds
        .stream()
        .map(id -> this.pool.submit(() -> this.acquireDeleting(connection, id)))
        .collect(Collectors.toList());

    promises.forEach(future -> {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new UncheckedSQLException(new SQLException(e));
      }
    });

    if (log.isDebugEnabled()) {
      log.debug("status=acquireDeletingUsingThreads, records={}, time={}", recordIds.size(),
          stopWatch.getDisplayTime()
      );
    }
  }

  @Override
  public void acquireDeletingUsingIn(Connection connection, List<UUID> recordIds) {
    final StopWatch stopWatch = StopWatch.createStarted();
    if (recordIds.isEmpty()) {
      if (log.isTraceEnabled()) {
        log.trace("status=noRecordsToDelete");
      }
      return;
    }
    int skip = 0;
    while (true) {

      final List<String> params = this.subList(recordIds, skip);
      final StringBuilder sql = new StringBuilder("DELETE FROM TTO_RECORD WHERE IDT_TTO_RECORD IN (")
          .append(this.buildBinds(params))
          .append(")");
      if (params.isEmpty()) {
        break;
      }

      try (final PreparedStatement stm = connection.prepareStatement(sql.toString())) {

        skip += params.size();

        for (int i = 1; i <= params.size(); i++) {
          stm.setString(i, params.get(i - 1));
        }
        final int affected = stm.executeUpdate();
        Validator.isTrue(
            affected == params.size(),
            "Didn't delete all records, expected=%d, actual=%d",
            params.size(), affected
        );
      } catch (SQLException e) {
        throw new UncheckedSQLException(e);
      }

      if (log.isDebugEnabled()) {
        log.debug("status=acquireDeleteUsingIn, records={}, time={}", recordIds.size(), stopWatch.getDisplayTime());
      }
    }

  }

  @Override
  public void acquireDeletingUsingBatch(Connection connection, List<UUID> recordIds) {
    if (recordIds.isEmpty()) {
      if (log.isTraceEnabled()) {
        log.trace("status=noRecordsToDelete");
      }
      return;
    }
    final StopWatch stopWatch = StopWatch.createStarted();
    try (Statement stm = connection.createStatement()) {
      for (final UUID recordId : recordIds) {
        stm.addBatch(String.format("DELETE FROM TTO_RECORD WHERE IDT_TTO_RECORD = '%s'", recordId));
      }
      final int affected = stm.executeBatch().length;
      Validator.isTrue(
          affected == recordIds.size(),
          "Couldn't delete records, expected=%d, records=%s", affected, recordIds
      );
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    }
    if (log.isDebugEnabled()) {
      log.debug("status=batchDeleted, records={}, time={}", recordIds.size(), stopWatch.getDisplayTime());
    }
  }

  @Override
  public void acquireDeleting(Connection connection, UUID id) {
    final String sql = "DELETE FROM TTO_RECORD WHERE IDT_TTO_RECORD = ? ";
    try (PreparedStatement stm = connection.prepareStatement(sql)) {
      stm.setString(1, String.valueOf(id));
      final int affected = stm.executeUpdate();
      if (log.isTraceEnabled()) {
        log.trace("m=acquireDeleting, status=deleted, id={}, affected={}", id, affected);
      }
      Validator.isTrue(affected == 1, "Couldn't delete record: %s", id);
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    }
  }

  @Override
  public void changeStatusToProcessed(Connection connection, List<UUID> ids) {
    ids.forEach(id -> this.changeStatusToProcessed(connection, id));
  }

  @Override
  public void changeStatusToProcessed(Connection connection, UUID id) {
    if (log.isTraceEnabled()) {
      log.trace("status=changing-status, id={}", id);
    }
    if (log.isDebugEnabled()) {
      log.trace("status=status-changed, id={}", id);
    }
  }

  private PreparedStatement createStreamingStatement(Connection con, String sql, int fetchSize) throws SQLException {
    final PreparedStatement stm = con.prepareStatement(
        sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY
    );
    stm.setFetchSize(fetchSize); // records to pull by time, in other words, the buffer size
//    stm.setMaxRows(BATCH_SIZE * 5); // the maximum records this statement can retrieve at all, nao setar senao cai
//    a performance da query
    return stm;
  }

  private UUID fillStatement(ProducerRecord record, PreparedStatement stm) throws SQLException {
    final UUID id = UUID.randomUUID();
    stm.setString(1, id.toString());
    stm.setString(2, record.getTopic());
    stm.setObject(3, record.getPartition());
    stm.setString(4, Base64.encodeToString(record.getKey()));
    stm.setString(5, Base64.encodeToString(record.getValue()));
    stm.setString(6, HeadersConverter.encodeBase64(record.getHeaders()));
    return id;
  }

  private List<String> subList(List<UUID> recordIds, int skip) {
    return recordIds
        .stream()
        .skip(skip)
        .limit(999)
        .map(UUID::toString)
        .collect(Collectors.toList());
  }

  private String buildBinds(List<String> params) {
    return params.stream()
        .map(it -> "?")
        .collect(Collectors.joining(", "));
  }
}
