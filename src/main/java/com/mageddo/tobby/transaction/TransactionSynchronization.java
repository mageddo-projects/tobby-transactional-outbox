package com.mageddo.tobby.transaction;

public interface TransactionSynchronization {
  /**
   * Execute this callback after commit the database transaction.
   *
   * @see TransactionSynchronizationManager
   */
  void afterCommit();
}
