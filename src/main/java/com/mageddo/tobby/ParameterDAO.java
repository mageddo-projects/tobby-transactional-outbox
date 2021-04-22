package com.mageddo.tobby;

import java.sql.Connection;
import java.time.LocalDateTime;

public interface ParameterDAO {

  LocalDateTime findAsDateTime(Connection connection, Parameter parameter,
      LocalDateTime defaultValue);

  String find(Connection connection, Parameter parameter, String defaultValue);

  void insertOrUpdate(Connection connection, Parameter parameter, String value);

  void insert(Connection connection, Parameter parameter, String value);

  int update(Connection connection, Parameter parameter, String value);

  boolean insertIfAbsent(Connection conn, Parameter parameter, String value);
}
