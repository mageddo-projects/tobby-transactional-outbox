package com.mageddo.tobby;

import java.sql.Connection;
import java.time.Duration;
import java.time.LocalDateTime;

import javax.inject.Inject;
import javax.inject.Singleton;

import static com.mageddo.db.ConnectionUtils.useTransaction;

@Singleton
public class Locker {

  private final LockDAO lockDAO;
  private final ParameterDAO parameterDAO;

  @Inject
  public Locker(LockDAO lockDAO, ParameterDAO parameterDAO) {
    this.lockDAO = lockDAO;
    this.parameterDAO = parameterDAO;
  }

  public void lock(Connection conn){
    useTransaction(conn, () -> {
      this.parameterDAO.insertOrUpdate(conn, Parameter.LOCK, LocalDateTime.now());
    });
    this.lockDAO.lock(conn, Duration.ofSeconds(2));
  }

}
