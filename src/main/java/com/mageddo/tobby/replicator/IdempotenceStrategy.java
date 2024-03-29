package com.mageddo.tobby.replicator;

public enum IdempotenceStrategy {
  /**
   * Search for records that are at TTO_RECORD but aren't at TTO_RECORD_PROCESSED, then try to insert records
   * on TTO_RECORD_PROCESSED, if success try to send to kafka, if success commit the transaction.
   */
  INSERT,

  /**
   * Search for record at TTO_RECORD, try to delete this record, if had success try to send to kafka, if had success
   * commit the transaction.
   */
  DELETE,

  /**
   * Search for record at TTO_RECORD, as {@link #DELETE}, try to delete records but after that also inserts the deleted
   * record at TTO_RECORD_PROCESSED table, this way history records are ensured.
   */
  DELETE_WITH_HISTORY,

  /**
   * Works just like {@link IdempotenceStrategy#DELETE} but do the delete in batch
   * considering ReplicatorConfig#getBufferSize()
   */
  BATCH_DELETE,

  /**
   * Works just like {@link IdempotenceStrategy#DELETE} but do the delete in batches across multiple threads
   */
  BATCH_PARALLEL_DELETE,

  /**
   * Search for records which are at TTO_RECORD with IND_STATUS=WAIT, divide them in multiple threads,
   * change the status to OK, then try to send to kafka, if success commit the transaction otherwise rollback and try
   * that batch again.
   */
  BATCH_PARALLEL_UPDATE
}
