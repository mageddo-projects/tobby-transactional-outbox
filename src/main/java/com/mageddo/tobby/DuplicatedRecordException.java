package com.mageddo.tobby;

import java.sql.SQLException;
import java.util.UUID;

import com.mageddo.db.DB;
import com.mageddo.db.SqlErrorCodes;

public class DuplicatedRecordException extends RuntimeException {
  public DuplicatedRecordException(UUID id, SQLException e) {
    super(String.format("%s: %s", id, e));
  }

  public static RuntimeException check(DB db, UUID id, SQLException e) {
    final boolean duplicateKeyError = SqlErrorCodes
        .of(db)
        .isDuplicateKeyError(e);
    if (!duplicateKeyError) {
      return new UncheckedSQLException(e);
    }
    return new DuplicatedRecordException(id, e);
  }
}
