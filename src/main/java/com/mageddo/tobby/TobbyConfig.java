package com.mageddo.tobby;

import javax.inject.Singleton;
import javax.sql.DataSource;

import com.mageddo.tobby.factory.SerializerCreator;
import com.mageddo.tobby.internal.utils.Validator;
import com.mageddo.tobby.producer.ProducerJdbc;
import com.mageddo.tobby.producer.kafka.SimpleJdbcKafkaProducerAdapter;

import org.apache.kafka.common.serialization.Serializer;

import dagger.Component;
import dagger.Module;
import dagger.Provides;

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

  public ProducerJdbc producerJdbc();

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

  @Module
  public static class ConnectionBasedModule {

    private final DataSource dataSource;

    public ConnectionBasedModule(DataSource dataSource) {
      this.dataSource = dataSource;
    }

    @Provides
    @Singleton
    public RecordDAO recordDAO() {
      return new RecordDAOHsqldb();
    }

    @Provides
    @Singleton
    public ParameterDAO parameterDAO() {
      return new ParameterDAOUniversal();
    }

    @Provides
    @Singleton
    ProducerJdbc producerJdbc(RecordDAO recordDAO){
      return new ProducerJdbc(recordDAO, this.dataSource);
    }
  }

  static TobbyConfig build(DataSource dataSource) {
    Validator.isTrue(dataSource != null, "Data source can't be null", dataSource);
    return DaggerTobbyConfig.builder()
        .connectionBasedModule(new ConnectionBasedModule(dataSource))
        .build();
  }
}
