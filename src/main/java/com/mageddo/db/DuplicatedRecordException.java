package com.mageddo.db;

import com.mageddo.tobby.UncheckedSQLException;

import java.sql.SQLException;
import java.util.UUID;

public class DuplicatedRecordException extends RuntimeException {
  public DuplicatedRecordException(UUID id, SQLException e) {
    super(String.format("%s: %s", id, e));
  }

  public static RuntimeException check(DB db, String id, SQLException e) {
    return check(db, UUID.nameUUIDFromBytes(id.getBytes()), e);
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
