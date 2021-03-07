package com.mageddo.tobby;

import java.sql.SQLException;

public class UncheckedSQLException extends RuntimeException {

  private final SQLException delegate;

  public UncheckedSQLException(SQLException delegate) {
    super(delegate);
    this.delegate = delegate;
  }

  public SQLException getDelegate() {
    return delegate;
  }

  public static UncheckedSQLException wrap(SQLException e){
    throw new UncheckedSQLException(e);
  }
}
