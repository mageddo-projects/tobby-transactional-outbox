package com.mageddo.tobby.producer.kafka;

import com.mageddo.tobby.producer.Producer;
import com.mageddo.tobby.producer.kafka.converter.ProducedRecordConverter;
import com.mageddo.tobby.producer.kafka.converter.ProducerRecordConverter;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;

public class JdbcKafkaProducer<K, V> {

  private final Producer delegate;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;

  public JdbcKafkaProducer(Producer delegate, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    this.delegate = delegate;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  public RecordMetadata send(ProducerRecord<K,V> record) {
    return ProducedRecordConverter.toMetadata(this.delegate.send(
        ProducerRecordConverter.of(
            this.keySerializer, this.valueSerializer, record
        )
    ));
  }
}
