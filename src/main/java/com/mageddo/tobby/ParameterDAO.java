package com.mageddo.tobby;

import java.time.LocalDateTime;

public interface ParameterDAO {

  String findAsText(Parameter parameter);

  LocalDateTime findAsDateTime(Parameter parameter);

  void update(Parameter parameter, LocalDateTime value);
}
