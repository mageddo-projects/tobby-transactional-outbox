package com.mageddo.tobby.producer;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

@Value
@Builder
public class ProducerConfig {

  @NonNull
  private DataSource dataSource;

  @NonNull
  @Builder.Default
  private Map<String, Object> producerConfigs = buildDefaultKafkaProducerConfigs();

  public static ProducerConfig from(DataSource dataSource) {
    return ProducerConfig
        .builder()
        .dataSource(dataSource)
        .producerConfigs(buildDefaultKafkaProducerConfigs())
        .build();
  }

  private static Map<String, Object> buildDefaultKafkaProducerConfigs() {
    final Map<String, Object> props = new HashMap<>();
    props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    return props;
  }
}
