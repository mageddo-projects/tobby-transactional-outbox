package com.mageddo.db;

import java.sql.SQLException;

import com.mageddo.tobby.UncheckedSQLException;

public class QueryTimeoutException extends UncheckedSQLException {

  public QueryTimeoutException(SQLException e) {
    super(e);
  }

  public static RuntimeException check(DB db, SQLException e) {
    final SqlErrorCodes sqlErrorCodes = SqlErrorCodes.of(db);
    if (sqlErrorCodes.isQueryTimeoutError(e)) {
      throw new QueryTimeoutException(e);
    }
    throw new UncheckedSQLException(e);
  }
}
