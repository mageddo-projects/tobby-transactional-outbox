package com.mageddo;

import java.sql.Connection;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.ProducerRecord;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.replicator.Replicators;

public interface RecordRecordCustomTableDAO extends RecordDAO {

  /**
   * Javadoc are reserved to explain smart tips about something isn't clear,
   * won't describe the obvious here.
   */
  ProducedRecord find(Connection connection, UUID id, String table);

  /**
   * Stores Record at the database table to be replicated
   * by {@link Replicators} later.
   */
  ProducedRecord save(Connection connection, ProducerRecord record, String table);

  /**
   * Finds all not replicated ProducedRecords, streaming from database in batches
   * to increase performance.
   *  @param connection
   * @param fetchSize
   * @param consumer   callback to be called for each record
   * @param from       time from which table will be scanned
   */
  void iterateNotProcessedRecordsUsingInsertIdempotence(
      Connection connection, int fetchSize, Consumer<ProducedRecord> consumer, LocalDateTime from, String table
  );

  void iterateOverRecords(
      Connection connection, int fetchSize, Consumer<ProducedRecord> consumer, String table
  );

  void iterateOverRecordsInWaitingStatus(
      Connection connection, int fetchSize, Duration timeToWaitBeforeReplicate,
      Consumer<ProducedRecord> consumer, String table
  );

  void changeStatusToProcessed(Connection connection, List<ProducedRecord> records, String changeAgent, String table);

  void changeStatusToProcessed(Connection connection, ProducedRecord record, String changeAgent, String table);

}
