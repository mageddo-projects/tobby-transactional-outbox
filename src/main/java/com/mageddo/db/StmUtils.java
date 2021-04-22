package com.mageddo.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.mageddo.tobby.UncheckedSQLException;
import com.mageddo.tobby.internal.utils.Threads;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
public class StmUtils {

  private static final ExecutorService EXECUTOR = Threads.newPool(5);

  public static int executeOrCancel(PreparedStatement stm, Duration timeout) throws SQLException {
    final AtomicBoolean semaphore = new AtomicBoolean();
    EXECUTOR.submit(() -> {
      final long now = System.currentTimeMillis();
      if (log.isTraceEnabled()) {
        log.trace("status=stmWatcherStarted");
      }
      while (!semaphore.get() && !timeHasExpired(timeout, now)) {
        if (!sleep()) {
          break;
        }
      }
      if (log.isTraceEnabled()) {
        log.trace("status=exitSleep");
      }
      if (semaphore.compareAndSet(false, true)) {
        try {
          if (log.isTraceEnabled()) {
            log.trace("status=cancellingStatement");
          }
          stm.cancel();
          if (log.isTraceEnabled()) {
            log.trace("status=canceled");
          }
        } catch (SQLException e) {
          throw new UncheckedSQLException(e);
        }
      }
    });
    final int affected = stm.executeUpdate();
    if (log.isTraceEnabled()) {
      log.trace("status=executed");
    }
    semaphore.set(true);
    return affected;
  }

  private static boolean sleep() {
    try {
      TimeUnit.MILLISECONDS.sleep(1000 / 60);
      return true;
    } catch (InterruptedException e) {
      return false;
    }
  }

  private static boolean timeHasExpired(Duration timeout, long now) {
    return System.currentTimeMillis() - now >= timeout.toMillis();
  }
}
