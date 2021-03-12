package com.mageddo.tobby.factory;

import java.time.Duration;

import javax.sql.DataSource;

import com.mageddo.tobby.ParameterDAO;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.replicator.IdempotenceStrategy;
import com.mageddo.tobby.replicator.ReplicatorFactory;

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

  public ReplicatorFactory create(Producer<byte[], byte[]> producer) {
    return this.create(producer, Duration.ZERO, DEFAULT_MAX_RECORD_DELAY_TO_COMMIT);
  }

  public ReplicatorFactory create(
      Producer<byte[], byte[]> producer, Duration idleTimeout, Duration maxRecordDelayToCommit
  ) {
    return create(producer, idleTimeout, maxRecordDelayToCommit, IdempotenceStrategy.DELETE);
  }

  public ReplicatorFactory create(
      Producer<byte[], byte[]> producer, Duration idleTimeout, Duration maxRecordDelayToCommit,
      IdempotenceStrategy strategy
  ) {
    return new ReplicatorFactory(
        producer, this.dataSource,
        this.recordDAO, this.parameterDAO,
        idleTimeout, maxRecordDelayToCommit,
        strategy
    );
  }
}
