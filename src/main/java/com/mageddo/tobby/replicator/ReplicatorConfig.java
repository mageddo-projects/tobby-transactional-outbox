package com.mageddo.tobby.replicator;

import java.time.Duration;

import javax.sql.DataSource;

import com.mageddo.tobby.replicator.idempotencestrategy.batchdelete.BatchDeleteIdempotenceStrategyConfig;

import org.apache.kafka.clients.producer.Producer;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * Configs for messages replication.
 */
@Value
@Builder(toBuilder = true, builderClassName = "ReplicatorConfigBuilder")
public class ReplicatorConfig {

  public static final Duration DEFAULT_MAX_RECORD_DELAY_TO_COMMIT = Duration.ofMinutes(15);

  /**
   * Producer used to send messages to kafka server.
   */
  @NonNull
  private final Producer<byte[], byte[]> producer;

  @NonNull
  private final DataSource dataSource;

  /**
   * How long stay waiting new records to be inserted at the database to be replicated,
   * if no record comes, stops the program.
   * <br><br>
   * Disabled by default setting value to {@link Duration#ZERO}.
   */
  @NonNull
  @Builder.Default
  private final Duration idleTimeout = Duration.ZERO;

  @NonNull
  @Builder.Default
  private final IdempotenceStrategy idempotenceStrategy = IdempotenceStrategy.DELETE;

  /**
   * The amount of time Replicator will look back to find not replicated Records comparing the
   * DAT_CREATED of the last replicated Record.
   * <p>
   * This configuration is necessary because the date which is set to the record at the database,
   * is the date of the moment INSERT was executed, not committed, it means if that commit holds
   * e.g 10 minutes, then there is a big chance of more recent records be committed before,
   * it can occur for a matter of milliseconds though, no difference here.
   */
  @NonNull
  @Builder.Default
  private final Duration maxRecordDelayToCommit = DEFAULT_MAX_RECORD_DELAY_TO_COMMIT;

  /**
   * How many messages to store on buffer call call KafkaProducer, it will increase
   * kafka produce speed because it will make use of kafka batch send, keep in mind that the higher the
   * buffer size, you are also holding messages to be sent to the Kafka broker, also increasing the chance
   * of Kafka send fails then Tobby will try send the entire the buffer again.
   */
  @Builder.Default
  private final int bufferSize = 20_000;

  /**
   * How many rows to JDBC driver retrieve from the database at once and store to local memory using ResultSet
   */
  @Builder.Default
  private final int fetchSize = 50_000;

  @NonNull
  @Builder.Default
  private final BatchDeleteIdempotenceStrategyConfig deleteIdempotenceStrategyConfig =
      BatchDeleteIdempotenceStrategyConfig.defaultConfig();

//  private final RecordDAO recordDAO;
//
//  private final ParameterDAO parameterDAO;
//
//  private final RecordProcessedDAO recordProcessedDAO;

  public static class ReplicatorConfigBuilder {}

}
