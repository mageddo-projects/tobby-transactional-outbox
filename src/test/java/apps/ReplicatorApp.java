package apps;

import java.util.Map;
import java.util.concurrent.Executors;

import com.mageddo.tobby.Tobby;
import com.mageddo.tobby.replicator.IdempotenceStrategy;
import com.mageddo.tobby.replicator.ReplicatorConfig;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import testing.DBMigration;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

public class ReplicatorApp {
  public static void main(String[] args) {
    final var kafkaProducer = new KafkaProducer<>(
        Map.of(
            BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
        ),
        new ByteArraySerializer(),
        new ByteArraySerializer()
    );
    final var tobby = Tobby.build(DBMigration.migratePostgres(10));
    final var replicator = tobby.replicator(ReplicatorConfig
        .builder()
        .producer(kafkaProducer)
        .idempotenceStrategy(IdempotenceStrategy.DELETE_WITH_HISTORY)
        .build()
    );

    final var poolSize = 5;
    final var pool = Executors.newFixedThreadPool(poolSize);
    for (int i = 0; i < poolSize; i++) {
      pool.submit(()  -> {
        replicator.replicateLocking();
      });
    }

  }

}
