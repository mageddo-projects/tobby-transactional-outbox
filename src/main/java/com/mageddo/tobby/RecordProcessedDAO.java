package com.mageddo.tobby;

import java.sql.Connection;

public interface RecordProcessedDAO {
  void save(Connection con, ProducedRecord record);
}
