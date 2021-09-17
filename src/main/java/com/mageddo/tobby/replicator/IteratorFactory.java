package com.mageddo.tobby.replicator;

import java.sql.Connection;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.mageddo.tobby.ParameterDAO;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.RecordProcessedDAO;
import com.mageddo.tobby.replicator.idempotencestrategy.batchdelete.BatchDeleteIdempotenceBasedReplicator;
import com.mageddo.tobby.replicator.idempotencestrategy.batchdelete.BatchParallelDeleteIdempotenceBasedReplicator;
import com.mageddo.tobby.replicator.idempotencestrategy.batchdelete.DeleteMode;

import static com.mageddo.tobby.replicator.ReplicatorConfig.REPLICATORS_BATCH_DELETE_DELETE_MODE;

@Singleton
public class IteratorFactory {

  private final RecordDAO recordDAO;
  private final ParameterDAO parameterDAO;
  private final RecordProcessedDAO recordProcessedDAO;
  private final BatchParallelDeleteIdempotenceBasedReplicator batchParallelDeleteIdempotenceBasedReplicator;
  private final UpdateIdempotenceBasedReplicator updateIdempotenceBasedReplicator;

  @Inject
  public IteratorFactory(RecordDAO recordDAO, ParameterDAO parameterDAO, RecordProcessedDAO recordProcessedDAO,
      BatchParallelDeleteIdempotenceBasedReplicator batchParallelDeleteIdempotenceBasedReplicator,
      UpdateIdempotenceBasedReplicator updateIdempotenceBasedReplicator) {
    this.recordDAO = recordDAO;
    this.parameterDAO = parameterDAO;
    this.recordProcessedDAO = recordProcessedDAO;
    this.batchParallelDeleteIdempotenceBasedReplicator = batchParallelDeleteIdempotenceBasedReplicator;
    this.updateIdempotenceBasedReplicator = updateIdempotenceBasedReplicator;
  }

  public StreamingIterator create(
      BufferedReplicator replicator,
      Connection readConn, Connection writeConn,
      ReplicatorConfig config
  ) {
    switch (config.getIdempotenceStrategy()) {
      case INSERT:
        return new InsertIdempotenceBasedReplicator(
            readConn, writeConn, this.recordDAO, this.parameterDAO,
            replicator, config.getMaxRecordDelayToCommit(),
            config.getFetchSize()
        );
      case DELETE:
        return new DeleteIdempotenceBasedReplicator(
            readConn, writeConn, this.recordDAO,
            replicator, config.getFetchSize()
        );
      case DELETE_WITH_HISTORY:
        return new DeleteWithHistoryIdempotenceBasedReplicator(
            readConn, writeConn, this.recordDAO, this.recordProcessedDAO,
            replicator, config.getFetchSize()
        );
      case BATCH_DELETE:
        return new BatchDeleteIdempotenceBasedReplicator(
            readConn, writeConn, this.recordDAO,
            replicator, config.getFetchSize(),
            DeleteMode.valueOf(config.get(REPLICATORS_BATCH_DELETE_DELETE_MODE))
        );
      case BATCH_PARALLEL_DELETE:
        return this.batchParallelDeleteIdempotenceBasedReplicator;
      case BATCH_PARALLEL_UPDATE:
        return this.updateIdempotenceBasedReplicator;
      default:
        throw new IllegalArgumentException("Not strategy implemented for: " + config.getIdempotenceStrategy());
    }
  }
}
