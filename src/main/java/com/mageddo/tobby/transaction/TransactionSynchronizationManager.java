package com.mageddo.tobby.transaction;

import java.util.ArrayList;
import java.util.List;

public class TransactionSynchronizationManager {

  private static final ThreadLocal<List<TransactionSynchronization>> syncs = ThreadLocal.withInitial(ArrayList::new);

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
