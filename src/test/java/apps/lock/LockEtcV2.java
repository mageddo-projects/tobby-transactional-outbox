package apps.lock;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LockEtcV2 {

  public static final int SECONDS_TIMEOUT = 5;

  public static void main(String[] args) throws SQLException, IOException {
    final var dataSource = dataSource(1);
    lock(dataSource);
    log.info("program finished");
  }


  public static void lock(DataSource dataSource) throws SQLException {
    try (final Connection con = dataSource.getConnection()) {
      acquireLock(con);
      con.commit();
      acquireLock(con);
    }
  }

  private static boolean acquireLock(Connection con) throws SQLException {
    final StringBuilder sql = new StringBuilder()
        .append("UPDATE TTO_PARAMETER SET \n")
        .append("  VAL_PARAMETER = CURRENT_TIMESTAMP \n")
        .append("WHERE IDT_TTO_PARAMETER = 'REPLICATOR_LOCK' \n");
    try (final PreparedStatement stm = con.prepareStatement(sql.toString())) {
      stm.setQueryTimeout(SECONDS_TIMEOUT);
      return stm.executeUpdate() == 1;
    }
  }

  public static DataSource dataSource(int size) throws IOException {

    Properties props = new Properties();
    props.load(LockEtcV2.class.getResourceAsStream("/db.properties"));

    final HikariConfig config = new HikariConfig();
    config.setMinimumIdle(size);
    config.setAutoCommit(false);
    config.setMaximumPoolSize(size);
    config.setDriverClassName(props.getProperty("driverClassName"));
    config.setJdbcUrl(props.getProperty("jdbcUrl"));
    config.setUsername(props.getProperty("username"));
    config.setPassword(props.getProperty("password"));
    return new HikariDataSource(config);
  }

}
