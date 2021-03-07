package com.mageddo.tobby;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Base64;
import java.util.UUID;
import java.util.function.Consumer;

import com.mageddo.tobby.converter.ProducedRecordConverter;

public class RecordDAOGeneric implements RecordDAO {

  public static final int BATCH_SIZE = 1000;

  @Override
  public ProducedRecord save(Connection connection, ProducerRecord record) {
    final StringBuilder sql = new StringBuilder()
        .append("INSERT INTO TTO_RECORD ( \n")
        .append("  IDT_TTO_RECORD, NAM_TOPIC, NUM_PARTITION, \n")
        .append("  TXT_KEY, TXT_VALUE, JSN_HEADERS \n")
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
      stm.setTimestamp(1, Timestamp.valueOf(from));
      try (ResultSet rs = stm.executeQuery()) {
        while (rs.next()) {
          consumer.accept(ProducedRecordConverter.map(rs));
        }
      }
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    }
  }

  private PreparedStatement createStm(Connection con) throws SQLException {
    final PreparedStatement stm = con.prepareStatement(
        "SELECT * FROM TTO_RECORD WHERE DAT_CREATED > ? ORDER BY DAT_CREATED ASC",
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY
    );
    stm.setFetchSize(BATCH_SIZE);
    return stm;
  }


  private UUID fillStatement(ProducerRecord record, PreparedStatement stm) throws SQLException {
    final UUID id = UUID.randomUUID();
    stm.setString(1, id.toString());
    stm.setString(2, record.getTopic());
    stm.setObject(3, record.getPartition());
    stm.setString(4, base64Encode(record.getKey()));
    stm.setString(5, base64Encode(record.getValue()));
    stm.setString(6, encode(record.getHeaders()));
    return id;
  }

  private String encode(Headers headers) {
    return null;
  }

  private String base64Encode(byte[] data) {
    return Base64
        .getEncoder()
        .encodeToString(data);
  }
}
