package com.mageddo.tobby;

import com.mageddo.tobby.replicator.ReplicatorFactory;

import java.sql.Connection;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.function.Consumer;

public interface RecordDAO {

  /**
   * Javadoc are reserved to explain smart tips about something isn't clear, ]
   * won't describe the obvious here.
   */
  ProducedRecord find(Connection connection, UUID id);

  /**
   * Stores Record at the database table to be replicated
   * by {@link ReplicatorFactory} later.
   */
  ProducedRecord save(Connection connection, ProducerRecord record);

  /**
   * Finds all not replicated ProducedRecords, streaming from database in batches
   * to increase performance.
   *
   * @param connection
   * @param consumer   callback to be called for each record
   * @param from       time from which table will be scanned
   */
  void iterateNotProcessedRecordsUsingInsertIdempotence(
      Connection connection, Consumer<ProducedRecord> consumer, LocalDateTime from
  );

  /**
   * Try acquire record to make sure only this worker will replicate it to kafka,
   * otherwise {@link DuplicatedRecordException} will be thrown.
   */
  void acquireInserting(Connection connection, UUID id);

  void iterateOverRecords(
      Connection connection, Consumer<ProducedRecord> consumer
  );

  void acquireDeleting(Connection connection, UUID id);
}
