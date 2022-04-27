package com.mageddo.tobby.producer.kafka.converter;

import com.mageddo.tobby.ProducerRecord;
import com.mageddo.tobby.internal.utils.StringUtils;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.UUID;

import static com.mageddo.tobby.Headers.TOBBY_EVENT_ID;
import static com.mageddo.tobby.internal.utils.KafkaHeaders.lastHeaderAsTextOrNull;

public class ProducerRecordConverter {

  private ProducerRecordConverter() {
  }

  public static <K, V> ProducerRecord of(
      Serializer<K> keySerializer, Serializer<V> valueSerializer,
      org.apache.kafka.clients.producer.ProducerRecord<K, V> record
  ) {
    return ProducerRecord
        .builder()
        .id(parseEventId(record.headers()))
        .topic(record.topic())
        .partition(record.partition())
        .key(keySerializer.serialize(record.topic(), record.key()))
        .value(valueSerializer.serialize(record.topic(), record.value()))
        .headers(HeadersConverter.fromKafkaHeaders(record))
        .build()
        ;
  }

  private static UUID parseEventId(Headers headers) {
    final String eventId = lastHeaderAsTextOrNull(headers, TOBBY_EVENT_ID);
    if (StringUtils.isBlank(eventId)) {
      return UUID.randomUUID();
    }
    return parseEventId(eventId);
  }

  private static UUID parseEventId(String eventId) {
    try {
      return UUID.fromString(eventId);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format(
          "Event ID=%s passed at %s header is not in UUID format, please check it",
          eventId, TOBBY_EVENT_ID
      ), e);
    }
  }

}
