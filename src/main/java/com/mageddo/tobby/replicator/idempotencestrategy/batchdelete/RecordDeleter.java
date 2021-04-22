package com.mageddo.tobby.replicator.idempotencestrategy.batchdelete;

import java.sql.Connection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.RecordDAO;

public class RecordDeleter {

  private final RecordDAO recordDAO;

  @Inject
  public RecordDeleter(RecordDAO recordDAO) {
    this.recordDAO = recordDAO;
  }

  public void delete(Connection con, List<ProducedRecord> records, DeleteMode deleteMode) {
    final List<UUID> recordIds = records
        .stream()
        .map(ProducedRecord::getId)
        .collect(Collectors.toList());
    switch (deleteMode) {
      case BATCH_DELETE:
        this.recordDAO.acquireDeletingUsingBatch(con, recordIds);
        break;
      case BATCH_DELETE_USING_IN:
        this.recordDAO.acquireDeletingUsingIn(con, recordIds);
        break;
      case BATCH_DELETE_USING_THREADS:
        this.recordDAO.acquireDeletingUsingThreads(con, recordIds);
        break;
    }
  }
}
