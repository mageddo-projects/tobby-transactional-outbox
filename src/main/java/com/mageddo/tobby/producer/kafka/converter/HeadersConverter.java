package com.mageddo.tobby.producer.kafka.converter;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HeadersConverter {

  public static Headers toKafkaHeaders(com.mageddo.tobby.Headers headers) {
    final RecordHeaders kafkaHeaders = new RecordHeaders();
    for (com.mageddo.tobby.Header header : headers.asList()) {
      kafkaHeaders.add(toKafkaHeader(header));
    }
    return kafkaHeaders;
  }

  private static Header toKafkaHeader(com.mageddo.tobby.Header header) {
    return new RecordHeader(header.getKey(), header.getValue());
  }

  public static <K, V> com.mageddo.tobby.Headers fromKafkaHeaders(ProducerRecord<K, V> record) {
    final List<com.mageddo.tobby.Header> headers = new ArrayList<>();
    for (Header header : record.headers()) {
      headers.add(fromKafkaHeader(header));
    }
    return new com.mageddo.tobby.Headers(headers);
  }

  public static com.mageddo.tobby.Header fromKafkaHeader(Header header) {
    return new com.mageddo.tobby.Header(header.key(), header.value());
  }
}
