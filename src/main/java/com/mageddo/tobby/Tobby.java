package com.mageddo.tobby;

import javax.sql.DataSource;

public class Tobby {
  public static TobbyConfig build(DataSource dataSource) {
    return TobbyConfig.build(dataSource);
  }
}
