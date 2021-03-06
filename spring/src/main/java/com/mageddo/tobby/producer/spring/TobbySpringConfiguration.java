package com.mageddo.tobby.producer.spring;

import javax.sql.DataSource;

import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.Tobby;
import com.mageddo.tobby.dagger.TobbyConfig;
import com.mageddo.tobby.factory.SerializerCreator;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;

@EnableKafka
@Configuration
@ConditionalOnProperty(value = "tobby.transactional.outbox.enabled", matchIfMissing = true)
@EnableConfigurationProperties(KafkaProperties.class)
public class TobbySpringConfiguration {

  @Bean
  public TobbyConfig tobbyConfig(DataSource dataSource) {
    return TobbyConfig.build(dataSource);
  }

  @Bean
  public Tobby tobby(TobbyConfig tobbyConfig) {
    return Tobby.builder()
        .tobbyConfig(tobbyConfig)
        .build();
  }

  @Bean
  public ProducerSpring producerSpring(RecordDAO recordDAO, DataSource dataSource) {
    return new ProducerSpring(recordDAO, dataSource);
  }

  @Bean
  public Producer<?, ?> simpleJdbcKafkaProducerAdapter(
      Tobby tobby,
      @Value("${spring.kafka.producer.key-serializer:}") String keySerializerClass,
      @Value("${spring.kafka.producer.value-serializer:}") String valueSerializerClass,
      KafkaProperties kafkaProperties,
      com.mageddo.tobby.producer.Producer producer
  ) {
    final Serializer<?> keySerializer = SerializerCreator.create(
        keySerializerClass, StringSerializer.class.getName()
    );
    final Serializer<?> valueSerializer = SerializerCreator.create(
        valueSerializerClass, StringSerializer.class.getName()
    );
    keySerializer.configure(kafkaProperties.buildProducerProperties(), true);
    valueSerializer.configure(kafkaProperties.buildProducerProperties(), false);
    return tobby.kafkaProducer(producer, keySerializer, valueSerializer);
  }

  @Bean
  @ConditionalOnProperty(value = "tobby.transactional.outbox.override-kafka-template", matchIfMissing = true)
  public KafkaTemplate<?, ?> kafkaTemplate(Producer<?, ?> producer) {
    return new KafkaTemplate<>(() -> producer);
  }

  @Bean
  public RecordDAO recordDAO(TobbyConfig tobbyConfig) {
    return tobbyConfig.recordDAO();
  }

}
