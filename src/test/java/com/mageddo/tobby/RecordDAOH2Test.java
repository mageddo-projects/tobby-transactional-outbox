package com.mageddo.tobby;

import javax.sql.DataSource;

import testing.DBMigration;

class RecordDAOH2Test extends RecordDAOTest {

  @Override
  DataSource dataSource() {
    return DBMigration.migrateEmbeddedH2();
  }
}
