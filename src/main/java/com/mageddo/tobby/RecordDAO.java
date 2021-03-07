package com.mageddo.tobby;

import java.time.LocalDateTime;
import java.util.function.Consumer;

public interface RecordDAO {

  ProducedRecord save(ProducerRecord record);

  void iterateNotProcessedRecords(Consumer<ProducedRecord> consumer, LocalDateTime from);
}
