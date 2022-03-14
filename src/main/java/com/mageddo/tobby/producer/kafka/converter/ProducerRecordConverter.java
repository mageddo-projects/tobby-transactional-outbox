package com.mageddo.tobby.producer.kafka.converter;

import com.mageddo.tobby.ProducerRecord;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.UUID;

public class ProducerRecordConverter {

  private ProducerRecordConverter() {
  }

  public static <K, V> ProducerRecord of(
      Serializer<K> keySerializer, Serializer<V> valueSerializer,
      org.apache.kafka.clients.producer.ProducerRecord<K, V> record
  ) {
    return ProducerRecord
        .builder()
        .id(fillId(record.headers()))
        .topic(record.topic())
        .partition(record.partition())
        .key(keySerializer.serialize(record.topic(), record.key()))
        .value(valueSerializer.serialize(record.topic(), record.value()))
        .headers(HeadersConverter.fromKafkaHeaders(record))
        .build()
        ;
  }

  private static <V, K> UUID fillId(Headers headers) {
    throw new UnsupportedOperationException();
  }

}
