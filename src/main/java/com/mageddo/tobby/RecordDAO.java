package com.mageddo.tobby;

import java.sql.Connection;
import java.time.LocalDateTime;
import java.util.function.Consumer;

public interface RecordDAO {

  ProducedRecord save(Connection connection, ProducerRecord record);

  void iterateNotProcessedRecords(Connection connection, Consumer<ProducedRecord> consumer, LocalDateTime from);
}
