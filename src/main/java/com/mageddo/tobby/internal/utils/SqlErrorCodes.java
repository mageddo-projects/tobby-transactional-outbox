package com.mageddo.tobby.internal.utils;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class SqlErrorCodes {

  private Set<Integer> duplicateKeyCodes;

  private static final Map<DB, SqlErrorCodes> data = new HashMap<>();

  public static SqlErrorCodes build(DB db) {
    switch (db) {
      case POSTGRES: {
        return put(
            db,
            SqlErrorCodes
                .builder()
                .duplicateKeyCodes(Sets.of(21000, 23505))
                .build()
        );
      }
      case MYSQL: {
        return put(
            db,
            SqlErrorCodes
                .builder()
                .duplicateKeyCodes(Sets.of(1062))
                .build()
        );
      }
      case HSQLDB: {
        return put(
            db,
            SqlErrorCodes
                .builder()
                .duplicateKeyCodes(Sets.of(-1))
                .build()
        );
      }
      case SQLSERVER: {
        return put(
            db,
            SqlErrorCodes
                .builder()
                .duplicateKeyCodes(Sets.of(2601, 2627))
                .build()
        );
      }
      case ORACLE:
        return put(
            db,
            SqlErrorCodes
                .builder()
                .duplicateKeyCodes(Sets.of(1))
                .build()
        );
      case H2: {
        return put(
            db,
            SqlErrorCodes
                .builder()
                .duplicateKeyCodes(Sets.of(23001, 23505))
                .build()
        );
      }
      default:
        return SqlErrorCodes.empty();
    }
  }

  private static SqlErrorCodes empty() {
    return new SqlErrorCodes(Sets.of());
  }

  private static SqlErrorCodes put(DB db, SqlErrorCodes errorCodes) {
    data.put(db, errorCodes);
    return errorCodes;
  }

  public static SqlErrorCodes of(DB db) {
    return data.get(db);
  }

  public boolean isDuplicateKeyError(SQLException e) {
    return isDuplicateKeyError(e.getErrorCode());
  }

  public boolean isDuplicateKeyError(int errorCode) {
    return this.duplicateKeyCodes.contains(errorCode);
  }
}
