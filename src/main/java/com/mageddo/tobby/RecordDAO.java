package com.mageddo.tobby;

import java.sql.Connection;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.function.Consumer;

public interface RecordDAO {

  ProducedRecord find(Connection connection, UUID id);

  ProducedRecord save(Connection connection, ProducerRecord record);

  void iterateNotProcessedRecords(Connection connection, Consumer<ProducedRecord> consumer, LocalDateTime from);
}
