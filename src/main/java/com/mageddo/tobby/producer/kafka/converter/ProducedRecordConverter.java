package com.mageddo.tobby.producer.kafka.converter;

import java.sql.Timestamp;
import java.util.zip.CRC32;

import com.mageddo.tobby.ProducedRecord;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

public class ProducedRecordConverter {

  private ProducedRecordConverter() {
  }

  public static RecordMetadata toMetadata(ProducedRecord record) {
    return new RecordMetadata(
        new TopicPartition(
            record.getTopic(), record.getPartition() == null ? -1 : record.getPartition()
        ),
        -1L,
        -1L,
        toMillis(record),
        digest(record),
        calcSize(record.getKey()),
        calcSize(record.getValue())
    );
  }

  public static org.apache.kafka.clients.producer.ProducerRecord<String, byte[]> toKafkaProducerRecord(
      ProducedRecord record
  ) {
    throw new UnsupportedOperationException();
  }

  private static long toMillis(ProducedRecord record) {
    if (record.getCreatedAt() == null) {
      return -1;
    }
    return Timestamp.valueOf(record.getCreatedAt())
        .toInstant()
        .toEpochMilli();
  }

  private static int calcSize(byte[] record) {
    return record == null ? 0 : record.length;
  }

  private static Long digest(ProducedRecord record) {
    final CRC32 digest = new CRC32();
    if (record.getKey() != null) {
      digest.update(record.getKey(), 0, record.getKey().length);
    }
    if (record.getValue() != null) {
      digest.update(record.getValue(), 0, record.getValue().length);
    }
    return digest.getValue();
  }
}
