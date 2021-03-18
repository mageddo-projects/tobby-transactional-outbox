package com.mageddo.tobby;

import com.mageddo.db.DB;
import com.mageddo.db.DBUtils;
import com.mageddo.db.SimpleDataSource;
import com.mageddo.db.SqlErrorCodes;
import com.mageddo.tobby.factory.DAOFactory;
import com.mageddo.tobby.factory.KafkaReplicatorFactory;
import com.mageddo.tobby.factory.SerializerCreator;
import com.mageddo.tobby.internal.utils.Validator;
import com.mageddo.tobby.producer.ProducerJdbc;
import com.mageddo.tobby.producer.kafka.JdbcKafkaProducerAdapter;
import com.mageddo.tobby.producer.kafka.SimpleJdbcKafkaProducerAdapter;
import com.mageddo.tobby.replicator.ReplicatorFactory;

import dagger.Binds;
import dagger.Component;
import dagger.Module;
import dagger.Provides;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serializer;

import javax.inject.Singleton;
import javax.sql.DataSource;

import java.time.Duration;

import static com.mageddo.tobby.factory.KafkaReplicatorFactory.DEFAULT_MAX_RECORD_DELAY_TO_COMMIT;

@Singleton
@Component(
    modules = {
        TobbyConfig.ConnectionBasedModule.class,
        TobbyConfig.BindsModule.class
    }
)
public interface TobbyConfig {

  com.mageddo.tobby.producer.Producer producer();

  KafkaReplicatorFactory replicatorFactory();

  RecordDAO recordDAO();

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

  default ReplicatorFactory replicator(Producer<byte[], byte[]> producer, Duration idleTimeout) {
    return this.replicator(producer, idleTimeout, DEFAULT_MAX_RECORD_DELAY_TO_COMMIT);
  }

  default ReplicatorFactory replicator(
      Producer<byte[], byte[]> producer, Duration idleTimeout, Duration maxRecordDelayToCommit
  ) {
    return this.replicatorFactory()
        .create(producer, idleTimeout, maxRecordDelayToCommit);
  }

  @Module
  public static class ConnectionBasedModule {

    private final DataSource dataSource;

    public ConnectionBasedModule(DataSource dataSource) {
      this.dataSource = dataSource;
    }

    @Provides
    @Singleton
    DB db() {
      final DB db = DBUtils.discoverDB(this.dataSource);
      SqlErrorCodes.build(db);
      return db;
    }

    @Provides
    @Singleton
    public RecordDAO recordDAO(DB db) {
      return DAOFactory.createRecordDao(db);
    }

    @Provides
    @Singleton
    public ParameterDAO parameterDAO() {
      return new ParameterDAOUniversal();
    }

    @Provides
    @Singleton
    ProducerJdbc producerJdbc(RecordDAO recordDAO) {
      return new ProducerJdbc(recordDAO, this.dataSource);
    }

    @Singleton
    @Provides
    KafkaReplicatorFactory kafkaReplicatorFactory(RecordDAO recordDAO, ParameterDAO parameterDAO) {
      return KafkaReplicatorFactory
          .builder()
          .dataSource(this.dataSource)
          .parameterDAO(parameterDAO)
          .recordDAO(recordDAO)
          .build();
    }
  }

  @Module
  public interface BindsModule {
    @Binds
    com.mageddo.tobby.producer.Producer producer(ProducerJdbc producerJdbc);
  }

  static TobbyConfig build(String url, String username, String password) {
    return build(new SimpleDataSource(url, password, username));
  }

  static TobbyConfig build(DataSource dataSource) {
    Validator.isTrue(dataSource != null, "Data source can't be null", dataSource);
    return DaggerTobbyConfig.builder()
        .connectionBasedModule(new ConnectionBasedModule(dataSource))
        .build();
  }

}
