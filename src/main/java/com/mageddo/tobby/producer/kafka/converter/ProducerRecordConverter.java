package com.mageddo.tobby.producer.kafka.converter;

import com.mageddo.tobby.ProducerRecord;

import org.apache.kafka.common.serialization.Serializer;

public class ProducerRecordConverter {

  private ProducerRecordConverter() {
  }

  public static <K, V> ProducerRecord of(
      Serializer<K> keySerializer, Serializer<V> valueSerializer,
      org.apache.kafka.clients.producer.ProducerRecord<K, V> record
  ) {
    return new ProducerRecord(
        record.topic(), record.partition(),
        keySerializer.serialize(record.topic(), record.key()),
        valueSerializer.serialize(record.topic(), record.value()),
        HeadersConverter.fromKafkaHeaders(record)
    );
  }

}
