package com.mageddo.db;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import com.mageddo.tobby.internal.utils.ObjectUtils;
import com.mageddo.tobby.internal.utils.StringUtils;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
public class SqlErrorCodes {

  private static final Map<DB, SqlErrorCodes> data = new HashMap<>();

  @NonNull
  private DB db;

  @Builder.Default
  private boolean useSqlStateForTranslation = false;

  @NonNull
  private Set<Integer> duplicateKeyCodes;

  @NonNull
  private Set<String> databaseProductName;

  @NonNull
  private Set<String> queryTimeoutErrors;

  public static SqlErrorCodes build(DB db) {
    final Properties codes = loadSqlErrorsProperties();
    for (Object key : codes.keySet()) {
      final String prefix = String.format("%s.", db.getName());
      final String parsedKey = (String) key;
      if (parsedKey.startsWith(prefix)) {
        return put(
            db,
            SqlErrorCodes
                .builder()
                .db(db)
                .useSqlStateForTranslation(toBoolean(codes, prefix, "useSqlStateForTranslation"))
                .duplicateKeyCodes(toIntegerSet(codes, prefix, "duplicateKeyCodes"))
                .databaseProductName(toStringSet(codes, prefix, "databaseProductName"))
                .queryTimeoutErrors(toStringSet(codes, prefix, "queryTimeoutErrors"))
                .build()
        );
      }
    }
    return empty();
  }

  public static SqlErrorCodes of(DB db) {
    return data.getOrDefault(db, empty());
  }

  public boolean isDuplicateKeyError(SQLException e) {
    if (this.useSqlStateForTranslation) {
      return this.isDuplicateKeyError(ObjectUtils.firstNonNull(parseIntOrNull(e.getSQLState()), e.getErrorCode()));
    }
    return isDuplicateKeyError(e.getErrorCode());
  }

  public boolean isDuplicateKeyError(int errorCode) {
    return this.duplicateKeyCodes.contains(errorCode);
  }

  public boolean isQueryTimeoutError(SQLException e) {
    final String msg = e.getMessage();
    for (String err : this.queryTimeoutErrors) {
      if (msg.contains(err)) {
        return true;
      }
    }
    return false;
  }

  private Integer parseIntOrNull(String text) {
    try {
      return Integer.parseInt(text);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static Set<Integer> toIntegerSet(String codes) {
    return Arrays.stream(codes.split(","))
        .map(Integer::parseInt)
        .collect(Collectors.toSet());
  }

  private static Set<Integer> toIntegerSet(Properties codes, String prefix, String key) {
    return toIntegerSet(getProperty(codes, prefix, key));
  }

  private static Set<String> toStringSet(String codes) {
    if (StringUtils.isBlank(codes)) {
      return Collections.emptySet();
    }
    return Arrays.stream(codes.split(","))
        .collect(Collectors.toSet());
  }

  private static Set<String> toStringSet(Properties codes, String prefix, String key) {
    return toStringSet(getProperty(codes, prefix, key));
  }

  private static boolean toBoolean(Properties codes, String prefix, String key) {
    final String v = getProperty(codes, prefix, key);
    if (StringUtils.isBlank(v)) {
      return false;
    }
    return Boolean.parseBoolean(v);
  }

  private static String getProperty(Properties codes, String prefix, String key) {
    return codes.getProperty(String.format("%s%s", prefix, key));
  }

  private static Properties loadSqlErrorsProperties() {
    try {
      final Properties properties = new Properties();
      loadIfNotNull(properties, "/com/mageddo/db/sql-error-codes.properties");
      loadIfNotNull(properties, "/com/mageddo/db/sql-error-codes-override.properties");
      return properties;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void loadIfNotNull(Properties properties, String path) throws IOException {
    final InputStream in = SqlErrorCodes.class.getResourceAsStream(path);
    if (in != null) {
      properties.load(in);
    }
  }

  private static SqlErrorCodes empty() {
    return SqlErrorCodes
        .builder()
        .db(DB.of("Unknown"))
        .duplicateKeyCodes(Collections.emptySet())
        .databaseProductName(Collections.emptySet())
        .queryTimeoutErrors(Collections.emptySet())
        .build();
  }

  private static SqlErrorCodes put(DB db, SqlErrorCodes errorCodes) {
    data.put(db, errorCodes);
    return errorCodes;
  }
}
