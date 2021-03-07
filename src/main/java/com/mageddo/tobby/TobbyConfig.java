package com.mageddo.tobby;

import java.time.Duration;

import javax.inject.Singleton;
import javax.sql.DataSource;

import com.mageddo.tobby.factory.DAOFactory;
import com.mageddo.tobby.factory.KafkaReplicatorFactory;
import com.mageddo.tobby.factory.SerializerCreator;
import com.mageddo.tobby.internal.utils.DB;
import com.mageddo.tobby.internal.utils.DBUtils;
import com.mageddo.tobby.internal.utils.SqlErrorCodes;
import com.mageddo.tobby.internal.utils.Validator;
import com.mageddo.tobby.producer.ProducerJdbc;
import com.mageddo.tobby.producer.kafka.SimpleJdbcKafkaProducerAdapter;
import com.mageddo.tobby.replicator.KafkaReplicator;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serializer;

import dagger.Component;
import dagger.Module;
import dagger.Provides;

import static com.mageddo.tobby.factory.KafkaReplicatorFactory.DEFAULT_MAX_RECORD_DELAY_TO_COMMIT;

@Singleton
@Component(
    modules = {
//        TobbyConfig.Modules.class
        TobbyConfig.ConnectionBasedModule.class
    }
)
public interface TobbyConfig {
//  @Module
//  interface Modules {
//    @Binds
//    @Singleton
//    FruitDAO bind(FruitDAOStdout impl);
//  }

  ProducerJdbc producerJdbc();

  default <K, V> SimpleJdbcKafkaProducerAdapter<K, V> jdbcProducerAdapter(
      Class<? extends Serializer<K>> keySerializer, Class<? extends Serializer<V>> valueSerializer
  ) {
    return new SimpleJdbcKafkaProducerAdapter<>(
        SerializerCreator.create(keySerializer, null),
        SerializerCreator.create(valueSerializer, null),
        this.producerJdbc()
    );
  }

  default <K, V> SimpleJdbcKafkaProducerAdapter<K, V> jdbcProducerAdapter(
      Serializer<K> keySerializer, Serializer<V> valueSerializer
  ) {
    return new SimpleJdbcKafkaProducerAdapter<>(keySerializer, valueSerializer, this.producerJdbc());
  }

  KafkaReplicatorFactory replicatorFactory();

  default KafkaReplicator replicator(Producer<byte[], byte[]> producer, Duration idleTimeout) {
    return this.replicator(producer, idleTimeout, DEFAULT_MAX_RECORD_DELAY_TO_COMMIT);
  }

  default KafkaReplicator replicator(
      Producer<byte[], byte[]> producer, Duration idleTimeout, Duration maxRecordDelayToCommit
  ) {
    return this.replicatorFactory()
        .create(producer, idleTimeout, maxRecordDelayToCommit);
  }

  RecordDAO recordDAO();

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

  static TobbyConfig build(DataSource dataSource) {
    Validator.isTrue(dataSource != null, "Data source can't be null", dataSource);
    return DaggerTobbyConfig.builder()
        .connectionBasedModule(new ConnectionBasedModule(dataSource))
        .build();
  }
}
