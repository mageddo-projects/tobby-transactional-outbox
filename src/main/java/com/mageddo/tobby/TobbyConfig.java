package com.mageddo.tobby;

import javax.inject.Singleton;
import javax.sql.DataSource;

import com.mageddo.db.DB;
import com.mageddo.db.DBUtils;
import com.mageddo.db.SimpleDataSource;
import com.mageddo.db.SqlErrorCodes;
import com.mageddo.tobby.factory.DAOFactory;
import com.mageddo.tobby.factory.ReplicatorProvider;
import com.mageddo.tobby.factory.SerializerCreator;
import com.mageddo.tobby.internal.utils.Validator;
import com.mageddo.tobby.producer.ProducerJdbc;
import com.mageddo.tobby.producer.kafka.JdbcKafkaProducerAdapter;
import com.mageddo.tobby.producer.kafka.SimpleJdbcKafkaProducerAdapter;
import com.mageddo.tobby.replicator.IteratorFactory;
import com.mageddo.tobby.replicator.ReplicatorConfig;
import com.mageddo.tobby.replicator.Replicators;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serializer;

import dagger.Binds;
import dagger.Component;
import dagger.Module;
import dagger.Provides;

@Singleton
@Component(
    modules = {
        TobbyConfig.ConnectionBasedModule.class,
        TobbyConfig.BindsModule.class
    }
)
public interface TobbyConfig {

  com.mageddo.tobby.producer.Producer producer();

  ReplicatorProvider replicatorProvider();

  IteratorFactory iteratorFactory();

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

  default Replicators replicator(ReplicatorConfig config) {
    return this.replicatorProvider().create(config);
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
    public ReplicatorProvider replicatorProvider(IteratorFactory iteratorFactory){
      return new ReplicatorProvider(this.dataSource, iteratorFactory);
    }

    @Provides
    @Singleton
    ProducerJdbc producerJdbc(RecordDAO recordDAO) {
      return new ProducerJdbc(recordDAO, this.dataSource);
    }

  }

  @Module
  public interface BindsModule {
    @Binds
    com.mageddo.tobby.producer.Producer producer(ProducerJdbc impl);

    @Binds
    RecordProcessedDAO recordProcessedDAO(RecordProcessedDAOGeneric impl);
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
