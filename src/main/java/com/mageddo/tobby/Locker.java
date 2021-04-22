package com.mageddo.tobby;

import java.sql.Connection;
import java.time.Duration;
import java.time.LocalDateTime;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.mageddo.db.DuplicatedRecordException;

import lombok.extern.slf4j.Slf4j;

import static com.mageddo.db.ConnectionUtils.useTransaction;

@Slf4j
@Singleton
public class Locker {

  private final LockDAO lockDAO;
  private final ParameterDAO parameterDAO;

  @Inject
  public Locker(LockDAO lockDAO, ParameterDAO parameterDAO) {
    this.lockDAO = lockDAO;
    this.parameterDAO = parameterDAO;
  }

  public void lock(Connection conn) {
    this.insertIfAbsent(conn);
    this.lockDAO.lock(conn, Duration.ofMillis(500));
    log.info("status=lockAcquired");
  }

  private void insertIfAbsent(Connection conn) {
    try {
      useTransaction(conn, () -> {
        this.parameterDAO.insertIfAbsent(conn, Parameter.REPLICATOR_LOCK, String.valueOf(LocalDateTime.now()));
      });
    } catch (DuplicatedRecordException e) {
      log.info("status=already-insert");
    }
  }

}
