package com.mageddo.tobby;

import java.sql.SQLException;
import java.util.UUID;

public class DuplicatedRecordException extends RuntimeException {
  public DuplicatedRecordException(UUID id, SQLException e) {
    super(String.format("%s: %s", id, e));
  }
}
