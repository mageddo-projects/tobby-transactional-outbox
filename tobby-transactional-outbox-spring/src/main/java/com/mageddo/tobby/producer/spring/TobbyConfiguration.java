package com.mageddo.tobby.producer.spring;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.UncheckedSQLException;
import com.mageddo.tobby.factory.DAOFactory;
import com.mageddo.tobby.factory.SerializerCreator;
import com.mageddo.tobby.internal.utils.DBUtils;
import com.mageddo.tobby.producer.ProducerJdbc;
import com.mageddo.tobby.producer.kafka.SimpleJdbcKafkaProducerAdapter;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
@ConditionalOnProperty(value = "tobby.transactional.outbox.enabled", matchIfMissing = true)
@EnableKafka
public class TobbyConfiguration {

  @Bean
  public ProducerSpring producerSpring(RecordDAO recordDAO, DataSource dataSource) {
    return new ProducerSpring(recordDAO, dataSource);
  }

  @Bean
  public SimpleJdbcKafkaProducerAdapter<?, ?> simpleJdbcKafkaProducerAdapter(
      RecordDAO recordDAO, DataSource dataSource,
      @Value("${spring.kafka.producer.key-serializer:}") String keySerializer,
      @Value("${spring.kafka.producer.value-serializer:}") String valueSerializer
  ) {
    return new SimpleJdbcKafkaProducerAdapter<>(
        SerializerCreator.create(keySerializer, StringSerializer.class.getName()),
        SerializerCreator.create(valueSerializer, StringSerializer.class.getName()),
        new ProducerJdbc(recordDAO, dataSource)
    );
  }

  @Bean
  @ConditionalOnProperty(value = "tobby.transactional.outbox.override-kafka-template", matchIfMissing = true)
  public KafkaTemplate<?, ?> kafkaTemplate(Producer<?, ?> producer) {
    return new KafkaTemplate<>(() -> producer);
  }

  @Bean
  public RecordDAO recordDAO(DataSource dataSource) {
    try (Connection connection = dataSource.getConnection()) {
      return DAOFactory.createRecordDao(DBUtils.discoverDB(connection));
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    }
  }



}
