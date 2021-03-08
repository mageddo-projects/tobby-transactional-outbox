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

  public static final Duration DEFAULT_MAX_RECORD_DELAY_TO_COMMIT = Duration.ofMinutes(15);

  DataSource dataSource;
  RecordDAO recordDAO;
  ParameterDAO parameterDAO;

  public KafkaReplicator create(Producer<byte[], byte[]> producer) {
    return this.create(producer, Duration.ZERO, DEFAULT_MAX_RECORD_DELAY_TO_COMMIT);
  }

  public KafkaReplicator create(
      Producer<byte[], byte[]> producer, Duration idleTimeout, Duration maxRecordDelayToCommit
  ) {
    return new KafkaReplicator(
        producer, this.dataSource,
        this.recordDAO, this.parameterDAO,
        idleTimeout, maxRecordDelayToCommit
    );
  }
}
