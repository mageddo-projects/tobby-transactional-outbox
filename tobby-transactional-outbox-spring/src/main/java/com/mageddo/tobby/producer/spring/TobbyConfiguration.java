package com.mageddo.tobby.producer.spring;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.UncheckedSQLException;
import com.mageddo.tobby.factory.DAOFactory;
import com.mageddo.tobby.internal.utils.DBUtils;
import com.mageddo.tobby.internal.utils.StringUtils;
import com.mageddo.tobby.producer.ProducerJdbc;
import com.mageddo.tobby.producer.kafka.SimpleJdbcKafkaProducerAdapter;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
//@ConditionalOnExpression(value =
//    "#{ ${tobby.transactional.outbox.enabled:true} == 'true' }"
//)
//@EnableConfigurationProperties(KafkaPro)
public class TobbyConfiguration {

  @Bean
  public ProducerSpring producerSpring(RecordDAO recordDAO, DataSource dataSource) {
    return new ProducerSpring(recordDAO, dataSource);
  }

  @Bean
  public SimpleJdbcKafkaProducerAdapter simpleJdbcKafkaProducerAdapter(
      RecordDAO recordDAO, DataSource dataSource,
      @Value("${spring.kafka.producer.key-serializer:}") String keySerializer,
      @Value("${spring.kafka.producer.value-serializer:}") String valueSerializer
  ) {
    return new SimpleJdbcKafkaProducerAdapter(
        createSerializerInstance(keySerializer, StringSerializer.class.getName()),
        createSerializerInstance(valueSerializer, StringSerializer.class.getName()),
        new ProducerJdbc(recordDAO, dataSource)
    );
  }

  @Bean
  public KafkaTemplate kafkaTemplate(SimpleJdbcKafkaProducerAdapter producer){
    return new KafkaTemplate(() -> producer);
  }

  @Bean
  public RecordDAO recordDAO(DataSource dataSource) {
    try (Connection connection = dataSource.getConnection()) {
      return DAOFactory.createRecordDao(DBUtils.discoverDB(connection));
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    }
  }

  private Serializer createSerializerInstance(String className, String defaultClass) {
    if(StringUtils.isBlank(className)){
      return (Serializer) newInstance(defaultClass);
    }
    return (Serializer) newInstance(className);
  }

  private Object newInstance(String keySerializer) {
    try {
      return Class.forName(keySerializer)
          .getDeclaredConstructor()
          .newInstance();
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }


}
