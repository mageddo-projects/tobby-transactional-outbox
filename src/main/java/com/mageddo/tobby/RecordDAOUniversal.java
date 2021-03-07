package com.mageddo.tobby;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.function.Consumer;

import com.mageddo.tobby.mapper.ProducedRecordMapper;

public class RecordDAOUniversal implements RecordDAO {

  public static final int BATCH_SIZE = 1000;

  @Override
  public ProducedRecord save(Connection connection, ProducerRecord record) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void iterateNotProcessedRecords(
      Connection connection, Consumer<ProducedRecord> consumer, LocalDateTime from
  ) {
    try (PreparedStatement stm = this.createStm(connection)) {
      stm.setTimestamp(1, Timestamp.valueOf(from));
      try (ResultSet rs = stm.executeQuery()) {
        while (rs.next()) {
          consumer.accept(ProducedRecordMapper.map(rs));
        }
      }
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    }
  }

  private PreparedStatement createStm(Connection con) throws SQLException {
    final PreparedStatement stm = con.prepareStatement(
        "SELECT * FROM RECORD WHERE DAT_CREATED > ? ORDER BY DAT_CREATED ASC",
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY
    );
    stm.setFetchSize(BATCH_SIZE);
    return stm;
  }
}
