package com.mageddo.tobby.replicator;

import com.mageddo.tobby.ParameterDAO;
import com.mageddo.tobby.RecordDAO;

import com.mageddo.tobby.RecordProcessedDAO;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import org.apache.kafka.clients.producer.Producer;

import javax.sql.DataSource;

import java.time.Duration;

@Value
@Builder(toBuilder = true)
public class ReplicatorConfig {

  public static final Duration DEFAULT_MAX_RECORD_DELAY_TO_COMMIT = Duration.ofMinutes(15);

  /**
   * Producer used to send messages to kafka server.
   */
  @NonNull
  private final Producer<byte[], byte[]> producer;

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

  private final DataSource dataSource;

//  private final RecordDAO recordDAO;
//
//  private final ParameterDAO parameterDAO;
//
//  private final RecordProcessedDAO recordProcessedDAO;

}
