package apps.lock;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LockEtc {

  public static final int SECONDS_TIMEOUT = 5;
  private static final ExecutorService executorService = Executors.newFixedThreadPool(1, r -> {
    Thread t = Executors
        .defaultThreadFactory()
        .newThread(r);
    t.setDaemon(true);
    return t;
  });

  public static void main(String[] args) throws SQLException, IOException {
    final var dataSource = LockEtcV2.dataSource(1);
    lock(dataSource);
    log.info("program finished");
  }


  public static void lock(DataSource dataSource) throws SQLException {
    try (final Connection con = dataSource.getConnection()) {
      acquireLock(con);
      con.commit();
    }
  }

  private static boolean acquireLock(Connection con) throws SQLException {
    final StringBuilder sql = new StringBuilder()
        .append("UPDATE TTO_PARAMETER SET \n")
        .append("  VAL_PARAMETER = CURRENT_TIMESTAMP \n")
        .append("WHERE IDT_TTO_PARAMETER = 'REPLICATOR_LOCK' \n");
    try (final PreparedStatement stm = con.prepareStatement(sql.toString());) {
//    stm.setQueryTimeout(SECONDS_TIMEOUT);
//    stm.cancel();
      final var completed = new AtomicBoolean(false);
      executorService.submit(() -> {
        try {
          log.info("sleeping");
          TimeUnit.SECONDS.sleep(SECONDS_TIMEOUT);
          log.info("wake");
          if (!completed.get()) {
            log.info("cancelling");
            stm.cancel();
            stm.close();
            log.info("canceled");
          }
        } catch (InterruptedException | SQLException e) {
          log.warn("error", e);
        }
      });
      log.info("executingQuery");
      final var locked = stm.executeUpdate() == 1;
      completed.set(true);
      log.info("executed Query");
      return locked;
    }
  }
}
