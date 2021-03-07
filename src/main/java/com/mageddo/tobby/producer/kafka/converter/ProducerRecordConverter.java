package com.mageddo.tobby.producer.kafka.converter;

import java.util.ArrayList;
import java.util.List;

import com.mageddo.tobby.Headers;
import com.mageddo.tobby.ProducerRecord;

import org.apache.kafka.common.header.Header;
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
        encodeHeaders(record)
    );
  }

  private static <K, V> Headers encodeHeaders(
      org.apache.kafka.clients.producer.ProducerRecord<K, V> record
  ) {
    final List<com.mageddo.tobby.Header> headers = new ArrayList<>();
    for (Header header : record.headers()) {
      headers.add(toHeader(header));
    }
    return new Headers(headers);
  }

  private static com.mageddo.tobby.Header toHeader(Header header) {
    return new com.mageddo.tobby.Header(header.key(), header.value());
  }

}
