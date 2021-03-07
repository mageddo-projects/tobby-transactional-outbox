package com.mageddo.tobby.adapters.kafka.converter;

import java.sql.Timestamp;

import com.mageddo.tobby.ProducedRecord;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

public class ProducedRecordConverter {

  private ProducedRecordConverter() {
  }

  public static RecordMetadata toMetadata(ProducedRecord record) {
    return new RecordMetadata(
        new TopicPartition(record.getTopic(), record.getPartition()),
        0L,
        (long) record.getId(),
        toMillis(record),
        digest(record),
        calcSize(record.getKey()),
        calcSize(record.getValue())
    );
  }

  private static long toMillis(ProducedRecord record) {
    return Timestamp.valueOf(record.getCreatedAt())
        .toInstant()
        .toEpochMilli();
  }

  private static int calcSize(byte[] record) {
    return record == null ? 0 : record.length;
  }

  private static Long digest(ProducedRecord record) {
    throw new UnsupportedOperationException();
  }
}
