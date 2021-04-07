package apps;

import java.util.concurrent.Executors;

import com.mageddo.tobby.Tobby;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import lombok.extern.slf4j.Slf4j;
import templates.KafkaProducerRecordTemplates;
import testing.DBMigration;

@Slf4j
public class ProducerApp {

  public static void main(String[] args) throws InterruptedException {
    final var tobby = Tobby.build(DBMigration.migrateAndGetDataSource(60));
    final var producer = tobby.kafkaProducer(
        StringSerializer.class, ByteArraySerializer.class
    );

    final var threads = 15;
    final var executorService = Executors.newFixedThreadPool(threads);
    for (int i = 0; i < threads; i++) {
      executorService.submit(() -> {
        try {
          final var stopWatch = new StopWatch();
          stopWatch.start();
          for (int j = 1; true; j++) {
            producer.send(KafkaProducerRecordTemplates.coconut());
            if (j % 300 == 0) {
              log.info("status=produced, count={}, thread={}, avg={}",
                  Thread
                      .currentThread()
                      .getName(),
                  String.format("%,d", j),
                  String.format("%,d", stopWatch.getTime() / j)
              );
            }
          }
        } catch (Exception e) {
          log.error("status=failed, msg={}", e.getMessage(), e);
        }
      });
    }
    Thread.currentThread()
        .join();
  }
}
