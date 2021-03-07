package testing;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DBMigration {

  public static DataSource migrateHSQLDB() {
    return migrate("jdbc:hsqldb:mem:testdb", null, null, "classpath:db/migration-hsqldb");
  }

  public static void migrate(DataSource dataSource, String... locations) {
    Flyway.configure()
        .dataSource(dataSource)
        .locations(locations)
        .load()
        .migrate();
  }

  public static DataSource migrate(String url, String user, String password, String... locations) {
    final var flyway = Flyway.configure()
        .dataSource(url, user, password)
        .locations(locations)
        .load();
    flyway.migrate();
    return flyway.getConfiguration()
        .getDataSource();
  }
}
