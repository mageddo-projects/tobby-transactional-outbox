package com.mageddo.tobby.factory;

import java.time.Duration;

import javax.sql.DataSource;

import com.mageddo.tobby.ParameterDAO;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.replicator.KafkaReplicator;

import org.apache.kafka.clients.producer.Producer;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class KafkaReplicatorFactory {

  DataSource dataSource;
  RecordDAO recordDAO;
  ParameterDAO parameterDAO;

  public KafkaReplicator create(Producer<byte[], byte[]> producer) {
    return this.create(producer, Duration.ZERO);
  }

  public KafkaReplicator create(Producer<byte[], byte[]> producer, Duration idleTimeout) {
    return new KafkaReplicator(producer, this.dataSource, this.recordDAO, this.parameterDAO, idleTimeout);
  }
}
