package com.mageddo.tobby;

import java.sql.Connection;
import java.time.LocalDateTime;

public interface ParameterDAO {

  LocalDateTime findAsDateTime(Connection connection, Parameter parameter,
      LocalDateTime defaultValue);

  void insertOrUpdate(Connection connection, Parameter parameter, LocalDateTime value);

  void insert(Connection connection, Parameter parameter, LocalDateTime value);

  int update(Connection connection, Parameter parameter, LocalDateTime value);
}
