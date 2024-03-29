package com.mageddo.tobby.producer;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

@Value
@Builder
public class ProducerConfig {

  @NonNull
  private DataSource dataSource;

  /**
   * Properties used to create a producer, used when {@link #producer} is null
   */
  @NonNull
  @Builder.Default
  private Map<String, Object> producerConfigs = buildDefaultKafkaProducerConfigs();

  /**
   * Produce used to post to kafka after save the tto_record, this property will override {@link #producerConfigs}
   *
   * @see ProducerEventuallyConsistent
   */
  private Producer<byte[], byte[]> producer;

  public static ProducerConfig from(DataSource dataSource) {
    return ProducerConfig
        .builder()
        .dataSource(dataSource)
        .producerConfigs(buildDefaultKafkaProducerConfigs())
        .build();
  }

  public static Map<String, Object> buildDefaultKafkaProducerConfigs() {
    final Map<String, Object> props = new HashMap<>();
    props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    props.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    return props;
  }
}
