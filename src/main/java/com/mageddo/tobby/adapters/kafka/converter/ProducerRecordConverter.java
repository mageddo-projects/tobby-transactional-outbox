package com.mageddo.tobby.adapters.kafka.converter;

import com.mageddo.tobby.Headers;
import com.mageddo.tobby.ProducerRecordReq;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;

public class ProducerRecordConverter {

  private ProducerRecordConverter() {
  }

  public static <K, V> ProducerRecordReq of(
      Serializer<K> keySerializer, Serializer<V> valueSerializer, ProducerRecord<K, V> record
  ) {
    return new ProducerRecordReq(
        record.topic(), record.partition(),
        keySerializer.serialize(record.topic(), record.key()),
        valueSerializer.serialize(record.topic(), record.value()),
        encodeHeaders(record)
    );
  }


  private static <K, V> Headers encodeHeaders(ProducerRecord<K, V> record) {
    throw new UnsupportedOperationException();
  }
}
