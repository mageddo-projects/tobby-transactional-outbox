package com.mageddo.db;

import java.sql.Connection;
import java.sql.SQLException;

import com.mageddo.tobby.UncheckedSQLException;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ConnectionUtils {
  public static void useTransaction(Connection con, Runnable runnable) {
    try {
      runnable.run();
      con.commit();
    } catch (SQLException e) {
      try {
        con.rollback();
        throw new UncheckedSQLException(e);
      } catch (SQLException e2) {
        throw new UncheckedSQLException(e2);
      }
    }
  }
}
