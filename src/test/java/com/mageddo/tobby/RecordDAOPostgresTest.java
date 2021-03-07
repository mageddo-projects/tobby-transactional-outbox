package com.mageddo.tobby;

import javax.sql.DataSource;

import org.junit.jupiter.api.extension.ExtendWith;

import testing.DBMigration;
import testing.PostgresExtension;

@ExtendWith(PostgresExtension.class)
class RecordDAOPostgresTest extends RecordDAOTest {

  @Override
  DataSource dataSource() {
    return DBMigration.migrateEmbeddedPostgres();
  }
}
