package com.mageddo.tobby;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.function.Consumer;

import com.mageddo.db.DB;
import com.mageddo.tobby.converter.HeadersConverter;
import com.mageddo.tobby.converter.ProducedRecordConverter;
import com.mageddo.tobby.internal.utils.Base64;

public class RecordDAOGeneric implements RecordDAO {

  public static final int BATCH_SIZE = 10000;

  private final DB db;

  public RecordDAOGeneric(DB db) {
    this.db = db;
  }

  @Override
  public ProducedRecord save(Connection connection, ProducerRecord record) {
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
  public void iterateNotProcessedRecords(
      Connection connection, Consumer<ProducedRecord> consumer, LocalDateTime from
  ) {
    try (PreparedStatement stm = this.createStm(connection)) {
      // prevent scanning too many future partitions
      final Timestamp to = Timestamp.valueOf(LocalDateTime.now()
          .plusDays(2));
      stm.setTimestamp(1, Timestamp.valueOf(from));
      stm.setTimestamp(2, to);
      stm.setTimestamp(3, Timestamp.valueOf(from));
      stm.setTimestamp(4, to);
      try (ResultSet rs = stm.executeQuery()) {
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
  public void acquire(Connection connection, UUID id) {
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

  private PreparedStatement createStm(Connection con) throws SQLException {
    final StringBuilder sql = new StringBuilder()
        .append("SELECT * FROM TTO_RECORD R \n")
        .append("WHERE DAT_CREATED > ? \n")
        .append("AND DAT_CREATED < ? \n")
        .append("AND NOT EXISTS ( \n")
        .append("  SELECT 1 FROM TTO_RECORD_PROCESSED \n")
        .append("  WHERE IDT_TTO_RECORD = R.IDT_TTO_RECORD \n")
        .append("  AND DAT_CREATED > ? \n")
        .append("  AND DAT_CREATED < ? \n")
        .append(") \n")
        .append("ORDER BY DAT_CREATED ASC \n");
    final PreparedStatement stm = con.prepareStatement(
        sql.toString(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY
    );
    stm.setFetchSize(BATCH_SIZE);
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

}
