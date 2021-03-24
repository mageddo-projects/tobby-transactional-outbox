package com.mageddo.tobby;

import com.mageddo.tobby.converter.ProducedRecordConverter;
import com.mageddo.tobby.converter.RecordProcessedMapper;
import com.mageddo.tobby.internal.utils.StopWatch;

import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

@Slf4j
@Singleton
public class RecordProcessedDAOGeneric implements RecordProcessedDAO {

  @Inject
  public RecordProcessedDAOGeneric() {
  }

  @Override
  public void save(Connection con, ProducedRecord record) {
    final StopWatch stopWatch = StopWatch.createStarted();
    final StringBuilder sql = new StringBuilder()
        .append("INSERT INTO TTO_RECORD_RECORD ( \n")
        .append("  IDT_TTO_RECORD, NAM_TOPIC, NUM_PARTITION, \n")
        .append("  TXT_KEY, TXT_VALUE, TXT_HEADERS \n")
        .append(") VALUES ( \n")
        .append("  ?, ?, ?, \n")
        .append("  ?, ?, ? \n")
        .append(") \n");
    try (final PreparedStatement stm = con.prepareStatement(sql.toString())) {
      RecordProcessedMapper.map(stm, record);
      stm.executeUpdate();
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    } finally {
      if (log.isTraceEnabled()) {
        log.trace("status=recordProcessedSaved, statementTime={}", stopWatch.getTime());
      }
    }

  }
}
