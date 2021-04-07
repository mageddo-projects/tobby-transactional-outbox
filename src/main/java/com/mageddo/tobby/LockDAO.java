package com.mageddo.tobby;

import java.sql.Connection;
import java.time.Duration;

public interface LockDAO {
  void lock(Connection conn, Duration timeout);
}
