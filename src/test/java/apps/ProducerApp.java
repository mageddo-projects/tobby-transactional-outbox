package apps;

import java.util.concurrent.Executors;

import com.mageddo.tobby.Tobby;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import lombok.extern.slf4j.Slf4j;
import templates.KafkaProducerRecordTemplates;

@Slf4j
public class ProducerApp {

  public static void main(String[] args) throws InterruptedException {
//    DBMigration.migratePostgres();
    final var tobby = Tobby.build(ReplicatorApp.dataSource(60));
    final var producer = tobby.kafkaProducer(
        StringSerializer.class, ByteArraySerializer.class
    );

    final var executorService = Executors.newFixedThreadPool(50);
    for (int i = 0; i < 50; i++) {
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
