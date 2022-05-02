package com.mageddo.tobby;

import java.sql.Connection;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import com.mageddo.db.DuplicatedRecordException;
import com.mageddo.tobby.replicator.Replicators;

public interface RecordDAO {

  /**
   * Javadoc are reserved to explain smart tips about something isn't clear, ]
   * won't describe the obvious here.
   */
  ProducedRecord find(Connection connection, UUID id);

  /**
   * Stores Record at the database table to be replicated
   * by {@link Replicators} later.
   */
  ProducedRecord save(Connection connection, ProducerRecord record);

  /**
   * Finds all not replicated ProducedRecords, streaming from database in batches
   * to increase performance.
   *  @param connection
   * @param fetchSize
   * @param consumer   callback to be called for each record
   * @param from       time from which table will be scanned
   */
  void iterateNotProcessedRecordsUsingInsertIdempotence(
      Connection connection, int fetchSize, Consumer<ProducedRecord> consumer, LocalDateTime from
  );

  /**
   * Try acquire record to make sure only this worker will replicate it to kafka,
   * otherwise {@link DuplicatedRecordException} will be thrown.
   */
  void acquireInserting(Connection connection, UUID id);

  void iterateOverRecords(
      Connection connection, int fetchSize, Consumer<ProducedRecord> consumer
  );

  void iterateOverRecordsInWaitingStatus(Connection connection, int fetchSize,
      Duration timeToWaitBeforeReplicate, Consumer<ProducedRecord> consumer);

  /**
   * @deprecated use multiple threads on a single connection won't guarantee parallelism because only a
   * statement can be ran at time.
   */
  @Deprecated
  void acquireDeletingUsingThreads(Connection connection, List<UUID> recordIds);

  /**
   * @deprecated Deleting using in is not a good strategy for Relational Database engines because it will be considered
   * different statements then it will cause performance decrease.
   */
  @Deprecated
  void acquireDeletingUsingIn(Connection connection, List<UUID> recordIds);

  void acquireDeletingUsingBatch(Connection connection, List<UUID> recordIds);

  void acquireDeleting(Connection connection, UUID id);

  void changeStatusToProcessed(Connection connection, List<ProducedRecord> records, String changeAgent);

  void changeStatusToProcessed(Connection connection, ProducedRecord record, String changeAgent);

  /**
   * @deprecated This method will not be effective on a world when there is a lot of records because it loads
   * all the records to the RAM
   */
  @Deprecated
  List<ProducedRecord> findAll(Connection connection);
}
