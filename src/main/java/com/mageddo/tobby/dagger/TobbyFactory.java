package com.mageddo.tobby.dagger;

import javax.inject.Singleton;
import javax.sql.DataSource;

import com.mageddo.db.SimpleDataSource;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.RecordProcessedDAO;
import com.mageddo.tobby.factory.SerializerCreator;
import com.mageddo.tobby.producer.ProducerConfig;
import com.mageddo.tobby.producer.kafka.JdbcKafkaProducerAdapter;
import com.mageddo.tobby.producer.kafka.SimpleJdbcKafkaProducerAdapter;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serializer;

import dagger.Component;

@Singleton
@Component(
    modules = {
        DaosProducersModule.class,
        DaosProducersBindsModule.class,
        ProducersModule.class
    }
)
public interface TobbyFactory {

  com.mageddo.tobby.producer.Producer producer();

  RecordDAO recordDAO();

  RecordProcessedDAO recordProcessedDAO();

  default <K, V> SimpleJdbcKafkaProducerAdapter<K, V> jdbcProducerAdapter(
      Class<? extends Serializer<K>> keySerializer, Class<? extends Serializer<V>> valueSerializer
  ) {
    return new SimpleJdbcKafkaProducerAdapter<>(
        SerializerCreator.create(keySerializer, null),
        SerializerCreator.create(valueSerializer, null),
        this.producer()
    );
  }

  default <K, V> SimpleJdbcKafkaProducerAdapter<K, V> jdbcProducerAdapter(
      Serializer<K> keySerializer, Serializer<V> valueSerializer
  ) {
    return this.jdbcProducerAdapter(keySerializer, valueSerializer, this.producer());
  }

  default <K, V> SimpleJdbcKafkaProducerAdapter<K, V> jdbcProducerAdapter(
      Serializer<K> keySerializer, Serializer<V> valueSerializer, com.mageddo.tobby.producer.Producer producer
  ) {
    return new SimpleJdbcKafkaProducerAdapter<>(keySerializer, valueSerializer, producer);
  }

  default <K, V> JdbcKafkaProducerAdapter<K, V> jdbcProducerAdapter(
      Producer<K, V> delegate, Serializer<K> keySerializer, Serializer<V> valueSerializer
  ) {
    return new JdbcKafkaProducerAdapter<>(delegate, this.jdbcProducerAdapter(keySerializer, valueSerializer));
  }

  default <K, V> JdbcKafkaProducerAdapter<K, V> jdbcProducerAdapter(
      Producer<K, V> delegate,
      Class<? extends Serializer<K>> keySerializer,
      Class<? extends Serializer<V>> valueSerializer
  ) {
    return new JdbcKafkaProducerAdapter<>(
        delegate,
        this.jdbcProducerAdapter(
            SerializerCreator.create(keySerializer, null),
            SerializerCreator.create(valueSerializer, null)
        )
    );
  }

  static TobbyFactory build(String url, String username, String password) {
    return build(new SimpleDataSource(url, password, username));
  }

  static TobbyFactory build(DataSource dataSource) {
    return build(ProducerConfig.from(dataSource));
  }

  static TobbyFactory build(ProducerConfig config) {
    return DaggerTobbyFactory.builder()
        .daosProducersModule(new DaosProducersModule(config.getDataSource()))
        .producersModule(new ProducersModule(config))
        .build();
  }

  com.mageddo.tobby.producer.ProducerJdbc jdbcProducer();
}
