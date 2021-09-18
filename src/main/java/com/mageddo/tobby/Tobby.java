package com.mageddo.tobby;

import javax.sql.DataSource;

import com.mageddo.tobby.dagger.TobbyFactory;
import com.mageddo.tobby.dagger.TobbyReplicatorConfig;
import com.mageddo.tobby.producer.InterceptableProducer;
import com.mageddo.tobby.producer.ProducerConfig;
import com.mageddo.tobby.replicator.ReplicatorConfig;
import com.mageddo.tobby.replicator.Replicators;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serializer;

import lombok.Builder;

@Builder
public class Tobby {

  private final TobbyFactory tobbyFactory;

  //
  // vanilla producers
  //
  public InterceptableProducer producer() {
    return this.tobbyFactory.producer();
  }

  //
  // kafka producer adapters
  //
  public <K, V> Producer<K, V> kafkaProducer(
      Class<? extends Serializer<K>> keySerializer, Class<? extends Serializer<V>> valueSerializer
  ) {
    return this.tobbyFactory.jdbcProducerAdapter(keySerializer, valueSerializer);
  }

  public <K, V> Producer<K, V> kafkaProducer(
      Producer<K, V> delegate,
      Class<? extends Serializer<K>> keySerializer,
      Class<? extends Serializer<V>> valueSerializer
  ) {
    return this.tobbyFactory.jdbcProducerAdapter(delegate, keySerializer, valueSerializer);
  }

  public <K, V> Producer<K, V> kafkaProducer(
      Serializer<K> keySerializer, Serializer<V> valueSerializer
  ) {
    return this.tobbyFactory.jdbcProducerAdapter(keySerializer, valueSerializer);
  }

  public <K, V> Producer<K, V> kafkaProducer(
      Producer<K, V> delegate, Serializer<K> keySerializer, Serializer<V> valueSerializer
  ) {
    return this.tobbyFactory.jdbcProducerAdapter(delegate, keySerializer, valueSerializer);
  }

  public <K, V> Producer<K, V> kafkaProducer(
      InterceptableProducer producer, Serializer<K> keySerializer, Serializer<V> valueSerializer
  ) {
    return this.tobbyFactory.jdbcProducerAdapter(keySerializer, valueSerializer, producer);
  }

  //
  // replicators
  //

  public static Replicators replicator(ReplicatorConfig config) {
    return TobbyReplicatorConfig.create(config);
  }

  public static Tobby build(DataSource dataSource) {
    return Tobby
        .builder()
        .tobbyFactory(TobbyFactory.build(dataSource))
        .build();
  }

  public static Tobby build(ProducerConfig config) {
    return Tobby
        .builder()
        .tobbyFactory(TobbyFactory.build(config))
        .build();
  }
}
