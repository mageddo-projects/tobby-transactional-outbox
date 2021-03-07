package com.mageddo.tobby;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.function.Consumer;

import javax.sql.DataSource;

import com.mageddo.tobby.mapper.ProducedRecordMapper;

public class RecordDAOUniversal implements RecordDAO {

  public static final int BATCH_SIZE = 1000;
  private final DataSource dataSource;

  public RecordDAOUniversal(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public ProducedRecord save(ProducerRecord record) {
    try (Connection con = this.dataSource.getConnection()) {
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    }
    return null;
  }

  @Override
  public void iterateNotProcessedRecords(Consumer<ProducedRecord> consumer, LocalDateTime from) {
    try (
        Connection con = this.dataSource.getConnection();
        PreparedStatement stm = this.createStm(con)
    ) {
      stm.setTimestamp(1, Timestamp.valueOf(from));
      final ResultSet rs = stm.executeQuery();
      while (rs.next()) {
        consumer.accept(ProducedRecordMapper.map(rs));
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
