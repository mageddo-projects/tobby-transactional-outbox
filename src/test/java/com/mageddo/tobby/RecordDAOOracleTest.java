package com.mageddo.tobby;

import javax.sql.DataSource;

import testing.DBMigration;

class RecordDAOOracleTest extends RecordDAOTest {
  @Override
  DataSource dataSource() {
    return DBMigration.migrateEmbeddedOracle();
  }
}
