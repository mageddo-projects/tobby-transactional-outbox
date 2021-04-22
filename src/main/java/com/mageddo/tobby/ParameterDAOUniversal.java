package com.mageddo.tobby;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.mageddo.db.DB;
import com.mageddo.db.DuplicatedRecordException;
import com.mageddo.db.QueryTimeoutException;
import com.mageddo.db.SqlErrorCodes;
import com.mageddo.db.StmUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.mageddo.db.ConnectionUtils.savepoint;

@Singleton
public class ParameterDAOUniversal implements ParameterDAO {

  private final Logger log = LoggerFactory.getLogger(getClass());
  private final DB db;

  @Inject
  public ParameterDAOUniversal(DB db) {
    this.db = db;
  }

  @Override
  public LocalDateTime findAsDateTime(
      Connection connection, Parameter parameter, LocalDateTime defaultValue
  ) {
    return LocalDateTime.parse(this.find(connection, parameter, defaultValue.toString()));
  }

  @Override
  public String find(Connection connection, Parameter parameter, String defaultValue) {
    try (
        final PreparedStatement stm = connection
            .prepareStatement("SELECT VAL_PARAMETER FROM TTO_PARAMETER WHERE IDT_TTO_PARAMETER = ?");
    ) {
      stm.setString(1, parameter.name());
      try (final ResultSet rs = stm.executeQuery()) {
        if (!rs.next()) {
          return defaultValue;
        }
        return rs.getString("VAL_PARAMETER");
      }
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    }
  }

  @Override
  public boolean insertIfAbsent(Connection conn, Parameter parameter, String value) {
    if (this.find(conn, parameter, null) == null) {
      this.insert(conn, parameter, value);
      return true;
    }
    return false;
  }

  @Override
  public void insertOrUpdate(Connection connection, Parameter parameter, String value) {
    try {
      savepoint(connection, () -> {
        if (this.update(connection, parameter, value) == 0) {
          log.info("status=nothing-to-update, action=inserting, parameter={}", parameter.name());
          try {
            this.insert(connection, parameter, value);
          } catch (DuplicatedRecordException e) {
            log.info("status=already-insert, parameter={}", parameter.name());
          }
        }
      });
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    }
  }

  @Override
  public void insert(Connection connection, Parameter parameter, String value) {
    final String sql = "INSERT INTO TTO_PARAMETER (IDT_TTO_PARAMETER, VAL_PARAMETER) VALUES (?, ?)";
    try (final PreparedStatement stm = connection.prepareStatement(sql)) {
      stm.setString(1, parameter.name());
      stm.setString(2, value);
      StmUtils.executeOrCancel(stm, Duration.ofMillis(500));
      log.info("status=inserted, parameter={}, value={}", parameter.name(), value);
    } catch (SQLException e) {
      if (SqlErrorCodes.isQueryTimeoutError(this.db, e)) {
        throw new QueryTimeoutException(sql, e);
      }
      throw DuplicatedRecordException.check(this.db, parameter.name(), e);
    }
  }

  @Override
  public int update(Connection connection, Parameter parameter, String value) {
    final StringBuilder sql = new StringBuilder()
        .append("UPDATE TTO_PARAMETER SET \n")
        .append("  VAL_PARAMETER = ?, \n")
        .append("  DAT_UPDATED = CURRENT_TIMESTAMP \n")
        .append("WHERE IDT_TTO_PARAMETER = ? \n");

    try (final PreparedStatement stm = connection.prepareStatement(sql.toString())) {
      stm.setString(1, value);
      stm.setString(2, parameter.name());
      return stm.executeUpdate();
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    }
  }
}
