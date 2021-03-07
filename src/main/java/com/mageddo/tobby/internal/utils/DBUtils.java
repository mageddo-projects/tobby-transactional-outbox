package com.mageddo.tobby.internal.utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DBUtils {

  private static final Pattern DB_NAME_REGEX = Pattern.compile("jdbc:(\\w+):");

  private DBUtils() {
  }

  public static DB discoverDB(Connection connection) throws SQLException {
    final String url = connection
        .getMetaData()
        .getURL();
    return DB.valueOf(findDBName(url).toUpperCase());
  }

  private static String findDBName(String jdbcUrl) {
    final Matcher matcher = DB_NAME_REGEX.matcher(jdbcUrl);
    Validator.isTrue(matcher.find(), "Couldn't parse jdbc url: %s", jdbcUrl);
    return matcher.group(1);
  }
}
