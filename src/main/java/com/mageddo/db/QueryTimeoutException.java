package com.mageddo.db;

import java.sql.SQLException;

import com.mageddo.tobby.UncheckedSQLException;

public class QueryTimeoutException extends UncheckedSQLException {

  private static final String ORACLE_QUERY_TIMEOUT_MSG = "user requested cancel of current operation";
  private static final String POSTGRES_QUERY_TIMEOUT_MSG = "canceling statement due to user request";
  private static final String[] QUERY_TIMEOUT_MSGS = {
      ORACLE_QUERY_TIMEOUT_MSG, POSTGRES_QUERY_TIMEOUT_MSG
  };

  public QueryTimeoutException(SQLException e) {
    super(e);
  }

  public static RuntimeException check(SQLException e) {
    final String msg = e.getMessage();
    for (final String queryTimeoutMsg : QUERY_TIMEOUT_MSGS) {
      if (msg.contains(queryTimeoutMsg)) {
        throw new QueryTimeoutException(e);
      }
    }
    throw new UncheckedSQLException(e);
  }
}
