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
    final var threads = 80;
    final var dc = DBMigration.migrateAndGetDataSource(80);
    final var tobby = Tobby.build(dc);
    final var producer = tobby.kafkaProducer(
        StringSerializer.class, ByteArraySerializer.class
    );

    final var executorService = Executors.newFixedThreadPool(threads);
    for (int i = 0; i < threads; i++) {
      executorService.submit(() -> {
        try {
          final var stopWatch = new StopWatch();
          stopWatch.start();
          for (int j = 1; true; j++) {
            producer.send(KafkaProducerRecordTemplates.coconut());
            final var threshold = 50;
            if (j % threshold == 0) {
              log.info("status=produced, total={}, batchRecords={}, totalTime={}, avgTime={}",
                  String.format("%,d", j),
                  String.format("%,d", threshold),
                  String.format("%,d", stopWatch.getTime()),
                  String.format("%,d", stopWatch.getTime() / threshold)
              );
              stopWatch.reset();
              stopWatch.start();
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
