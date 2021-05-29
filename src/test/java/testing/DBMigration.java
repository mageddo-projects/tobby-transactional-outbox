package testing;

import java.util.Properties;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.flywaydb.core.Flyway;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

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
        "classpath:com/mageddo/tobby/db/migration-postgres",
        "classpath:com/mageddo/testing/tobby/db/migration-postgres"
    );
  }

  @SneakyThrows
  public static DataSource migrateAndGetDataSource(int size) {
    final var props = new Properties();
    props.load(DBMigration.class.getResourceAsStream(System.getProperty(
        "db.properties",
        "/db.properties"
    )));
    final var dc = pgDataSource(size, props);
    migrate(
        dc.getJdbcUrl(),
        dc.getUsername(), dc.getPassword(),
        props.getProperty("locations")
    );
    return dc;
  }

  static HikariDataSource pgDataSource(int size, Properties props) {
    final var config = new HikariConfig();
    config.setDriverClassName(props.getProperty("driverClassName"));
    config.setMinimumIdle(size);
    config.setAutoCommit(false);
    config.setMaximumPoolSize(size);
    config.setJdbcUrl(props.getProperty("jdbcUrl"));
    config.setUsername(props.getProperty("username"));
    config.setPassword(props.getProperty("password"));
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
