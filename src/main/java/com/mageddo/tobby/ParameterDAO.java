package com.mageddo.tobby;

import java.sql.Connection;
import java.time.LocalDateTime;

public interface ParameterDAO {

  LocalDateTime findAsDateTime(Connection connection, Parameter parameter);

  void update(Connection connection, Parameter parameter, LocalDateTime value);
}
