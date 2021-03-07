package com.mageddo.tobby;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

public class RecordDAOUniversal implements RecordDAO {

  private final DataSource dataSource;

  public RecordDAOUniversal(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public ProducedRecord save(ProducerRecord record) {
    try (Connection con = this.dataSource.getConnection()){
    } catch (SQLException e){
      throw new UncheckedSQLException(e);
    }
    return null;
  }
}
