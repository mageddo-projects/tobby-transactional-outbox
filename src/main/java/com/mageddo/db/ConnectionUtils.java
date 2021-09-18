package com.mageddo.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;

import com.mageddo.tobby.UncheckedSQLException;
import com.mageddo.tobby.producer.ConnectionHandler;

import com.mageddo.tobby.transaction.TransactionSynchronizationManager;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.mageddo.tobby.producer.ConnectionHandlerExecutor.executeAfterCommitCallbacks;

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
      quietClose(con);
    }
  }

  @Deprecated
  public static <T> T runAndClose(ConnectionHandler handler, CallableForHandler<T> runnable) {
    final T r = runAndClose(handler.connection(), (con) -> runnable.call(handler));
    executeAfterCommitCallbacks(handler);
    return r;
  }

  public static <T> T runAndClose(Connection connection, Callable<T> runnable) {
    try {
      final T r = runnable.call(connection);
      final boolean autoCommit = connection.getAutoCommit();
      if (!autoCommit) {
        connection.commit();
      }
      TransactionSynchronizationManager.execute();
      return r;
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    } finally {
      quietClose(connection);
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

  public interface CallableForHandler<T> {
    T call(ConnectionHandler conn);
  }
}
