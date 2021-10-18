package com.mageddo.tobby.transaction;

import java.util.ArrayList;
import java.util.List;

public class TransactionSynchronizationManager {

  private static final ThreadLocal<List<TransactionSynchronization>> syncs = ThreadLocal.withInitial(ArrayList::new);

  /**
   * Add a transaction callback to the queue.
   * @param synchronization the callback which will be executed
   */
  public static void registerSynchronization(TransactionSynchronization synchronization) {
    syncs.get()
        .add(synchronization);
  }

  public static void execute() {
    syncs.get()
        .forEach(TransactionSynchronization::afterCommit);
    syncs.get()
        .clear();
  }
}
