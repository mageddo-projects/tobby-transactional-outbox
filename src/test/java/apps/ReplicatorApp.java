package apps;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.mageddo.tobby.Tobby;
import com.mageddo.tobby.replicator.IdempotenceStrategy;
import com.mageddo.tobby.replicator.ReplicatorConfig;

import com.mageddo.tobby.replicator.Replicators;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import lombok.extern.slf4j.Slf4j;
import testing.DBMigration;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

@Slf4j
public class ReplicatorApp {
  public static void main(String[] args) throws InterruptedException {
    final var kafkaProducer = new KafkaProducer<>(
        Map.of(
            BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
        ),
        new ByteArraySerializer(),
        new ByteArraySerializer()
    );
    final var tobby = Tobby.build(DBMigration.migrateAndGetDataSource(6));
    final var replicator = tobby.replicator(ReplicatorConfig
        .builder()
        .producer(kafkaProducer)
        .idempotenceStrategy(IdempotenceStrategy.DELETE_WITH_HISTORY)
        .idleTimeout(Duration.ofSeconds(10))
        .build()
    );

    final var poolSize = 5;
    final var pool = Executors.newFixedThreadPool(poolSize);
    for (int i = 0; i < poolSize; i++) {
      submit(replicator, pool);
    }
    while (true){
      submit(replicator, pool);
      Thread.sleep(10000);
    }

//    pool.shutdown();

  }

  private static void submit(Replicators replicator, ExecutorService pool) {
    pool.submit(() -> {
      try {
        replicator.replicateLocking();
        log.info("status=done");
      } catch (Throwable e) {
        log.error("status=fatal, msg={}", e.getMessage(), e);
      }
    });
  }

}
