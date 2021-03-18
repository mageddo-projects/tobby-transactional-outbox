package com.mageddo.tobby;

import java.time.Duration;

import javax.sql.DataSource;

import com.mageddo.tobby.replicator.ReplicatorFactory;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serializer;

import lombok.Builder;

@Builder
public class Tobby {

  private final TobbyConfig tobbyConfig;

  //
  // vanilla producers
  //
  public com.mageddo.tobby.producer.Producer producer(){
    return this.tobbyConfig.producer();
  }

  //
  // kafka producer adapters
  //
  public  <K, V> Producer<K, V> kafkaProducer(
      Class<? extends Serializer<K>> keySerializer, Class<? extends Serializer<V>> valueSerializer
  ) {
    return this.tobbyConfig.jdbcProducerAdapter(keySerializer, valueSerializer);
  }

  public  <K, V> Producer<K, V> kafkaProducer(
      Producer<K, V> delegate,
      Class<? extends Serializer<K>> keySerializer,
      Class<? extends Serializer<V>> valueSerializer
  ) {
    return this.tobbyConfig.jdbcProducerAdapter(delegate, keySerializer, valueSerializer);
  }

  public <K, V> Producer<K, V> kafkaProducer(
      Serializer<K> keySerializer, Serializer<V> valueSerializer
  ) {
    return this.tobbyConfig.jdbcProducerAdapter(keySerializer, valueSerializer);
  }

  public <K, V> Producer<K, V> kafkaProducer(
      Producer<K, V> delegate, Serializer<K> keySerializer, Serializer<V> valueSerializer
  ) {
    return this.tobbyConfig.jdbcProducerAdapter(delegate, keySerializer, valueSerializer);
  }

  public <K, V> Producer<K, V> kafkaProducer(
      com.mageddo.tobby.producer.Producer producer, Serializer<K> keySerializer, Serializer<V> valueSerializer
  ) {
    return this.tobbyConfig.jdbcProducerAdapter( keySerializer, valueSerializer, producer);
  }

  //
  // replicators
  //

  public ReplicatorFactory replicator(Producer<byte[], byte[]> producer) {
    return this.replicator(producer, Duration.ZERO);
  }

  public ReplicatorFactory replicator(Producer<byte[], byte[]> producer, Duration idleTimeout) {
    return this.tobbyConfig.replicator(producer, idleTimeout);
  }

  public ReplicatorFactory replicator(
      Producer<byte[], byte[]> producer, Duration idleTimeout, Duration maxRecordDelayToCommit
  ) {
    return this.tobbyConfig.replicator(producer, idleTimeout, maxRecordDelayToCommit);
  }

  public static Tobby build(DataSource dataSource) {
    return Tobby
        .builder()
        .tobbyConfig(TobbyConfig.build(dataSource))
        .build();
  }
}
