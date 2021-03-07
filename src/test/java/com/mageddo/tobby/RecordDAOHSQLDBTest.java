package com.mageddo.tobby;

import javax.sql.DataSource;

import testing.DBMigration;

class RecordDAOHSQLDBTest extends RecordDAOTest {
  @Override
  DataSource dataSource() {
    return DBMigration.migrateEmbeddedHSQLDB();
  }

  @Override
  void shutdown() {
    this.execute("SHUTDOWN");
  }
}
