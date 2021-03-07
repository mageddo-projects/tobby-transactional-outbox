package com.mageddo.tobby;

public interface RecordDAO {
  ProducedRecord save(ProducerRecord record);
}
