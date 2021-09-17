package apps;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.mageddo.tobby.Tobby;
import com.mageddo.tobby.internal.utils.Threads;
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
  public static void main(String[] args) {
    final var kafkaProducer = new KafkaProducer<>(
        Map.of(
            BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
        ),
        new ByteArraySerializer(),
        new ByteArraySerializer()
    );
    final var dataSource = DBMigration.migrateAndGetDataSource(10);
    final var replicator = Tobby.replicator(ReplicatorConfig
        .builder()
        .dataSource(dataSource)
        .producer(kafkaProducer)
        .idempotenceStrategy(IdempotenceStrategy.BATCH_PARALLEL_UPDATE)
        .idleTimeout(Duration.ofSeconds(5))
//        .fetchSize(1000)
//        .bufferSize(5000)
        .build()
    );

    final var poolSize = 5;
    final var pool = Executors.newFixedThreadPool(poolSize);
    for (int i = 0; i < poolSize; i++) {
      submit(replicator, pool);
    }
    while (true) {
      submit(replicator, pool);
      Threads.sleep(Duration.ofSeconds(10));
    }

//    pool.shutdown();

  }

  private static void submit(Replicators replicator, ExecutorService pool) {
    pool.submit(() -> {
      try {
        replicator.replicateLocking();
        log.info("status=finished");
      } catch (Throwable e) {
        log.error("status=fatal, msg={}", e.getMessage(), e);
      }
    });
  }

}
