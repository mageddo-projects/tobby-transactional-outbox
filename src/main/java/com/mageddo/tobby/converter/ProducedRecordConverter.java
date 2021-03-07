package com.mageddo.tobby.converter;

import java.sql.ResultSet;
import java.util.UUID;

import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.ProducerRecord;

public class ProducedRecordConverter {

  private ProducedRecordConverter() {
  }

  public static ProducedRecord map(ResultSet rs){
    throw new UnsupportedOperationException();
  }

  public static ProducedRecord from(UUID id, ProducerRecord record) {
    return new ProducedRecord(
        id,
        record.getTopic(),
        record.getPartition(),
        record.getKey(),
        record.getValue(),
        record.getHeaders(),
        null
    );
  }
}
