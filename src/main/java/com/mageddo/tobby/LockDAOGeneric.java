package com.mageddo.tobby;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.mageddo.db.DB;
import com.mageddo.db.QueryTimeoutException;
import com.mageddo.db.StmUtils;

@Singleton
public class LockDAOGeneric implements LockDAO {

  private final DB db;

  @Inject
  public LockDAOGeneric(DB db) {
    this.db = db;
  }

  @Override
  public void lock(Connection conn, Duration timeout) {
    final StringBuilder sql = new StringBuilder()
        .append("UPDATE TTO_PARAMETER SET \n")
        .append("  VAL_PARAMETER = CURRENT_TIMESTAMP \n")
        .append("WHERE IDT_TTO_PARAMETER = ? \n");
    try (final PreparedStatement stm = conn.prepareStatement(sql.toString())) {
      stm.setString(1, Parameter.REPLICATOR_LOCK.name());
//      final int affected = StmUtils.executeOrCancel(stm, timeout);
      stm.setQueryTimeout((int) (timeout.toMillis() / 1000));
      final int affected = stm.executeUpdate();
      if (affected != 1) {
        throw new IllegalStateException("Must update exactly one record while locking");
      }
    } catch (SQLException e) {
      throw QueryTimeoutException.check(this.db, e);
    }
  }
}
