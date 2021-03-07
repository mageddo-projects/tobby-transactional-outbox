package com.mageddo.tobby;

public interface RecordDAO {
  ProducedRecord save(ProducerRecordReq record);
}
