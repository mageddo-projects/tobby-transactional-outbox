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
    useTransaction(conn, () -> {
      try {
        this.parameterDAO.insert(conn, Parameter.REPLICATOR_LOCK, LocalDateTime.now());
      } catch (DuplicatedRecordException e) {
        log.debug("status=already-insert");
      }
    });
    this.lockDAO.lock(conn, Duration.ofSeconds(2));
    log.info("status=lockAcquired");
  }

}
