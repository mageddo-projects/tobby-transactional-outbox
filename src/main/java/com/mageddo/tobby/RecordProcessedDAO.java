package com.mageddo.tobby;

import java.sql.Connection;
import java.util.UUID;

public interface RecordProcessedDAO {

  void save(Connection con, ProducedRecord record);

  ProducedRecord find(Connection con, UUID id);
}
