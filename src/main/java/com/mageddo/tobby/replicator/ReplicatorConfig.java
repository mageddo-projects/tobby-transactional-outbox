package com.mageddo.tobby.replicator;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import javax.sql.DataSource;

import com.mageddo.tobby.replicator.idempotencestrategy.batchdelete.DeleteMode;

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
  public static final String REPLICATORS_BATCH_DELETE_DELETE_MODE = "replicators.batch-delete.delete-mode";

  public static final String REPLICATORS_BATCH_PARALLEL_DELETE_MODE = "replicators.batch-parallel-delete.delete-mode";
  public static final String REPLICATORS_BATCH_PARALLEL_THREADS =
      "replicators.batch-parallel-delete.threads";
  public static final String REPLICATORS_BATCH_PARALLEL_BUFFER_SIZE =
      "replicators.batch-parallel-delete.buffer-size";
  public static final String REPLICATORS_BATCH_PARALLEL_THREAD_BUFFER_SIZE =
      "replicators.batch-parallel-delete.thread-buffer-size";


  public static final String REPLICATORS_UPDATE_IDEMPOTENCE_THREADS =
      "replicators.update-idempotence.threads";
  public static final String REPLICATORS_UPDATE_IDEMPOTENCE_BUFFER_SIZE =
      "replicators.update-idempotence.buffer-size";
  public static final String REPLICATORS_UPDATE_IDEMPOTENCE_THREAD_BUFFER_SIZE =
      "replicators.update-idempotence.thread-buffer-size";

  public static final String REPLICATORS_UPDATE_IDEMPOTENCE_TIME_TO_WAIT_BEFORE_REPLICATE =
      "replicators.update-idempotence.time-to-wait-before-replicate";


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
  private final IdempotenceStrategy idempotenceStrategy = IdempotenceStrategy.BATCH_PARALLEL_UPDATE;

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
   * // todo must be moved to BufferedReplicatorConfigs
   */
  @Builder.Default
  private final int bufferSize = 20_000;

  /**
   * How many rows to JDBC driver retrieve from the database at once and store to local memory using ResultSet
   */
  @Builder.Default
  private final int fetchSize = 50_000;

  @NonNull
  private Map<String, String> props;

  /**
   * Some condition to check if replicator job should stop or not, it will be tested after each wave.
   *
   * Default it's to never stop
   *
   * @see ReplicatorStopPredicates
   */
  @NonNull
  @Builder.Default
  private Predicate<ReplicatorContextVars> stopPredicate = (it) -> false;

  public int getInt(String key) {
    this.validateIsSet(key);
    return Integer.parseInt(this.props.get(key));
  }

  public String get(String key) {
    this.validateIsSet(key);
    return this.props.get(key);
  }

  public Map<String, String> getProps() {
    return Collections.unmodifiableMap(this.props);
  }

  private void validateIsSet(String key) {
    if (!this.props.containsKey(key) || this.props.get(key) == null) {
      throw new IllegalStateException(String.format("Prop not set %s", key));
    }
  }

  public static class ReplicatorConfigBuilder {

    public ReplicatorConfigBuilder() {
      this.props = buildDefaultProps();
    }

    public ReplicatorConfigBuilder put(String key, String value) {
      this.props.put(key, value);
      return this;
    }


    private static Map<String, String> buildDefaultProps() {
      final Map<String, String> props = new HashMap<>();
      props.put(REPLICATORS_BATCH_DELETE_DELETE_MODE, DeleteMode.BATCH_DELETE_USING_IN.name());

      props.put(REPLICATORS_BATCH_PARALLEL_BUFFER_SIZE, String.valueOf(20_000));
      props.put(REPLICATORS_BATCH_PARALLEL_THREADS, "10");
      props.put(REPLICATORS_BATCH_PARALLEL_DELETE_MODE, DeleteMode.BATCH_DELETE_USING_IN.name());
      props.put(REPLICATORS_BATCH_PARALLEL_THREAD_BUFFER_SIZE, String.valueOf(1_000));

      props.put(REPLICATORS_UPDATE_IDEMPOTENCE_BUFFER_SIZE, String.valueOf(20_000));
      props.put(REPLICATORS_UPDATE_IDEMPOTENCE_THREADS, "10");
      props.put(REPLICATORS_UPDATE_IDEMPOTENCE_THREAD_BUFFER_SIZE, String.valueOf(1_000));
      props.put(REPLICATORS_UPDATE_IDEMPOTENCE_TIME_TO_WAIT_BEFORE_REPLICATE, "PT10M");
      return props;
    }
  }

}
