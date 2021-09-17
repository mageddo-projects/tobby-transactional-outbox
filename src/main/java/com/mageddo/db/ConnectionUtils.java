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
    useTransaction(con, (conn) -> {
      runnable.run();
      return null;
    });
  }

  public static <T> T useTransaction(Connection con, Callable<T> runnable) {
    try {
      final boolean isAutoCommit = con.getAutoCommit();
      if (isAutoCommit) {
        con.setAutoCommit(false);
      }
      final T r = runnable.call(con);
      if (isAutoCommit) {
        con.setAutoCommit(true);
      } else {
        con.commit();
      }
      return r;
    } catch (SQLException e) {
      ConnectionUtils.quietRollback(con);
      throw new UncheckedSQLException(e);
    } catch (Exception e) {
      ConnectionUtils.quietRollback(con);
      throw e;
    }
  }

  public static <T> T useTransactionAndClose(Connection con, Callable<T> runnable) {
    try {
      return useTransaction(con, runnable);
    } finally {
      try {
        con.close();
      } catch (SQLException e) {
        throw new UncheckedSQLException(e);
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

  public static void quietRollback(Connection conn) {
    try {
      if (!conn.getAutoCommit()) {
        conn.rollback();
      }
    } catch (SQLException e) {
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

  public interface Callable<T> {
    T call(Connection conn);
  }
}
