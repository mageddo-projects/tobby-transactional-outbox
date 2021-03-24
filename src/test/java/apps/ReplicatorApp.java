package apps;

import com.mageddo.tobby.Tobby;
import com.mageddo.tobby.replicator.ReplicatorConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import testing.DBMigration;

import javax.sql.DataSource;

import java.util.Map;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

public class ReplicatorApp {
  public static void main(String[] args) {
    DBMigration.migratePostgres();
    final var kafkaProducer = new KafkaProducer<>(
        Map.of(
            BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
        ),
        new ByteArraySerializer(),
        new ByteArraySerializer()
    );
    final var tobby = Tobby.build(dataSource(3));
    final var replicator = tobby.replicator(ReplicatorConfig
        .builder()
        .producer(kafkaProducer)
        .build()
    );
    replicator.replicate();
  }

  public static DataSource dataSource(int size) {
    final var config = new HikariConfig();
    config.setDriverClassName("org.postgresql.Driver");
    config.setMinimumIdle(size);
    config.setAutoCommit(false);
    config.setMaximumPoolSize(size);
    config.setJdbcUrl("jdbc:postgresql://localhost:5436/db?currentSchema=tobby2");
    config.setUsername("root");
    config.setPassword("root");
    return new HikariDataSource(config);
  }
}
