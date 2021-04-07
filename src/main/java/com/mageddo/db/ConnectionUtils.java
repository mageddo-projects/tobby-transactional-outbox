package com.mageddo.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;

import com.mageddo.tobby.UncheckedSQLException;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ConnectionUtils {
  public static void useTransaction(Connection con, Runnable runnable) {
    try {
      final boolean isAutoCommit = con.getAutoCommit();
      if (isAutoCommit) {
        con.setAutoCommit(false);
      }
      runnable.run();
      if (isAutoCommit) {
        con.setAutoCommit(true);
      } else {
        con.commit();
      }
    } catch (SQLException e) {
      try {
        ConnectionUtils.quietRollback(con);
        throw new UncheckedSQLException(e);
      } catch (SQLException e2) {
        throw new UncheckedSQLException(e2);
      }
    }
  }

  public static void savepoint(Connection con, Callback callback) throws SQLException {
    final Savepoint sp = con.setSavepoint("SAVEPOINT_1");
    try {
      callback.run();
    } catch (Exception e) {
      con.rollback(sp);
      throw e;
    }
  }

  public static void quietRollback(Connection conn) throws SQLException {
    boolean isClosed = true;
    try {
      isClosed = conn.isClosed();
    } catch (SQLException e) {
    }
    if (!isClosed) {
      conn.rollback();
    }
  }

  public static void quietClose(Connection conn) {
    try {
      conn.close();
    } catch (SQLException e) {

    }
  }

  public interface Callback {
    void run() throws SQLException;
  }
}
