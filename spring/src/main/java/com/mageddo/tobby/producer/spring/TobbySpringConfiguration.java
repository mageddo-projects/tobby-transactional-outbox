package com.mageddo.tobby.producer.spring;

import javax.sql.DataSource;

import com.mageddo.RecordRecordCustomTableDAO;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.Tobby;
import com.mageddo.tobby.dagger.TobbyFactory;
import com.mageddo.tobby.factory.SerializerCreator;
import com.mageddo.tobby.producer.ProducerConfig;
import com.mageddo.tobby.producer.ProducerEventuallyConsistent;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

@EnableKafka
@Configuration
@ConditionalOnProperty(value = "tobby.transactional.outbox.enabled", matchIfMissing = true)
@EnableConfigurationProperties({KafkaProperties.class, TobbyConfigProperties.class})
public class TobbySpringConfiguration {

  @Bean
  @ConditionalOnProperty(value = "tobby.transactional.outbox.auto-tobby-config", matchIfMissing = true)
  public Tobby.Config tobbyConfig(TobbyConfigProperties configProperties) {
    return configProperties.toConfig();
  }

  @Bean
  @ConditionalOnProperty(value = "tobby.transactional.outbox.auto-tobby-context", matchIfMissing = true)
  public TobbyFactory tobbyFactory(Tobby.Config config, ProducerConfig producerConfig) {
    return TobbyFactory.build(producerConfig, config);
  }

  @Bean
  @ConditionalOnMissingBean(ProducerConfig.class)
  @ConditionalOnProperty(value = "tobby.transactional.outbox.auto-producer-config", matchIfMissing = true)
  public ProducerConfig producerConfig(DataSource dataSource, KafkaProperties kafkaProperties) {

    final Map<String, Object> props = kafkaProperties.buildProducerProperties();
    props.put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    props.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

    return ProducerConfig
        .builder()
        .dataSource(dataSource)
        .producerConfigs(props)
        .build();
  }

  @Bean
  @ConditionalOnProperty(value = "tobby.transactional.outbox.auto-tobby", matchIfMissing = true)
  public Tobby tobby(TobbyFactory tobbyFactory) {
    return Tobby.builder()
        .tobbyFactory(tobbyFactory)
        .build();
  }

  @Bean
  public ProducerEventuallyConsistentSpring producerEventualConsistent(
      TobbyFactory tobbyFactory, DataSource dataSource, RecordDAO recordDAO
  ) {
    return new ProducerEventuallyConsistentSpring(
        dataSource,
        new ProducerEventuallyConsistent(tobbyFactory.kafkaProducer(), recordDAO, dataSource)
    );
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
  public RecordRecordCustomTableDAO recordDAO(TobbyFactory tobbyFactory) {
    return tobbyFactory.recordDAOCustomTable();
  }

  @Bean
  public TobbyProducerJMX tobbyProducerJMX(TobbyFactory tobbyFactory){
    return new TobbyProducerJMX(tobbyFactory.producerJMX());
  }

}
