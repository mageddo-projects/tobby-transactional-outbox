package testing;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.flywaydb.core.Flyway;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DBMigration {

  public static DataSource migrateEmbeddedH2() {
    return cleanAndMigrate(
        "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1", null, null,
        "classpath:com/mageddo/tobby/db/migration-h2"
    );
  }

  public static DataSource migrateEmbeddedHSQLDB() {
    return cleanAndMigrate(
        "jdbc:hsqldb:mem:testdb", null, null,
        "classpath:com/mageddo/tobby/db/migration-hsqldb"
    );
  }

  public static DataSource migrateEmbeddedOracle() {
    return cleanAndMigrate(
        "jdbc:hsqldb:mem:PUBLIC;sql.syntax_ora=true", null, null,
        "classpath:com/mageddo/tobby/db/migration-oracle"
    );
  }

  public static DataSource migrateEmbeddedPostgres() {
    return cleanAndMigrate(
        "jdbc:postgresql://localhost:5429/postgres?currentSchema=postgres",
        "postgres", "postgres",
        "classpath:com/mageddo/tobby/db/migration-postgres"
    );
  }

  public static DataSource migratePostgres(int size) {
    final var dc = pgDataSource(size);
//    migrate(
//        dc.getJdbcUrl(),
//        dc.getUsername(), dc.getPassword(),
//        "classpath:com/mageddo/tobby/db/migration-postgres"
//    );
    return dc;
  }

  static HikariDataSource pgDataSource(int size) {
    final var config = new HikariConfig();
    config.setDriverClassName("org.postgresql.Driver");
    config.setMinimumIdle(size);
    config.setAutoCommit(false);
    config.setMaximumPoolSize(size);
    config.setJdbcUrl("jdbc:postgresql://localhost:5432/db?currentSchema=stg_mg");
    config.setUsername("root");
    config.setPassword("root");
    return new HikariDataSource(config);
  }

  public static Flyway setup(String url, String user, String password, String... locations) {
    return Flyway.configure()
        .dataSource(url, user, password)
        .locations(locations)
        .load();
  }

  public static DataSource migrate(String url, String user, String password, String... locations) {
    return migrate(setup(url, user, password, locations));
  }

  public static DataSource migrate(Flyway flyway) {
    flyway.migrate();
    return flyway.getConfiguration()
        .getDataSource();
  }

  public static DataSource cleanAndMigrate(String url, String user, String password, String... locations) {
    return cleanAndMigrate(setup(url, user, password, locations));
  }

  public static DataSource cleanAndMigrate(Flyway flyway) {
    flyway.clean();
    flyway.migrate();
    return flyway.getConfiguration()
        .getDataSource();
  }
}
