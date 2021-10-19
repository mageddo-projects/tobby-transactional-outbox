package com.mageddo.tobby;

import javax.sql.DataSource;

import com.mageddo.tobby.dagger.TobbyFactory;
import com.mageddo.tobby.dagger.TobbyReplicatorConfig;
import com.mageddo.tobby.producer.ProducerConfig;
import com.mageddo.tobby.replicator.ReplicatorConfig;
import com.mageddo.tobby.replicator.Replicators;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serializer;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

@Builder
public class Tobby {

  private final TobbyFactory tobbyFactory;

  //
  // vanilla producers
  //
  public com.mageddo.tobby.producer.Producer producer() {
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
      com.mageddo.tobby.producer.Producer producer, Serializer<K> keySerializer, Serializer<V> valueSerializer
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

  public static Tobby build(ProducerConfig producerConfig) {
    return build(producerConfig, Config.theDefault());
  }

  public static Tobby build(ProducerConfig producerConfig, Config config) {
    return Tobby
        .builder()
        .tobbyFactory(TobbyFactory.build(producerConfig, config))
        .build();
  }

  /**
   * General Tobby configurations, for specific replication configurations, see {@link ReplicatorConfig}.
   */
  @Data
  @Builder(toBuilder = true)
  public static class Config {

    public static final String TOBBY_RECORD_TABLE_NAME_PROP = "tobby.record-table.name";

    /**
     * The table name which tobby will consider to create and replicate the records from.
     * Your table must be compliance with a specific DDL, see {@link RecordDAO}.
     *
     * This property also can be set by System Property {@value #TOBBY_RECORD_TABLE_NAME_PROP}
     */
    @NonNull
    @Builder.Default
    private String recordTableName = "TTO_RECORD";

    public static Config theDefault() {
      return Config
          .builder()
          .build();
    }
  }
}
