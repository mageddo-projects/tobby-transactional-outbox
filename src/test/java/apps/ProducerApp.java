package apps;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.mageddo.tobby.Tobby;
import com.mageddo.tobby.internal.utils.Threads;
import com.mageddo.tobby.producer.ProducerConfig;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import lombok.extern.slf4j.Slf4j;
import templates.KafkaProducerRecordTemplates;
import testing.DBMigration;

@Slf4j
public class ProducerApp {

  public static void main(String[] args) throws InterruptedException {

//    {
//      final var pool = Threads.newPool(20);
//      final var counter = new AtomicInteger();
//      for (int i = 0; i < 500_000; i++) {
//        StopWatch stopWatch = StopWatch.createStarted();
//        pool.submit(() -> {
//          counter.incrementAndGet();
//          try {
//            Thread.sleep((long) (Math.random() * 2.0));
//          } catch (InterruptedException e) {
//          }
//        });
//        log.info("status=submitted, i={}, time={}", i, stopWatch.getTime());
//      }
//      log.info("status=submited, actual={}", counter.get());
//      pool.shutdown();
//      pool.awaitTermination(5, TimeUnit.DAYS);
//      log.info("total={}", counter.get());
//    }
//
//    System.exit(0);


    final var recordsSent = new AtomicInteger();
    final var threads = 10;
    final var dc = DBMigration.migrateAndGetDataSource(70);
    final var tobby = Tobby.build(dc);
    final var producer = tobby.kafkaProducer(
        StringSerializer.class, ByteArraySerializer.class
    );

    final var kafkaProducer = new KafkaProducer<String, byte[]>(
        ProducerConfig.buildDefaultKafkaProducerConfigs());
    final var executorService = Threads.newPool(threads);
    for (int i = 0; i < threads; i++) {
      executorService.submit(() -> {
        try {
          final var stopWatch = new StopWatch();
          stopWatch.start();
          for (int j = 1; recordsSent.get() < 1_000_000; j++) {

//            kafkaProducer.send(KafkaProducerRecordTemplates.articles());
            final var stopWatch1 = StopWatch.createStarted();
            producer.send(KafkaProducerRecordTemplates.articles());
            recordsSent.incrementAndGet();
            log.trace("status=sent, time={}", stopWatch1.getTime());

            final var threshold = 50;
            if (j % threshold == 0) {
              log.trace("status=produced, recordsSent={}, total={}, batchRecords={}, totalTime={}, avgTime={}",
                  String.format("%,d", recordsSent.get()),
                  String.format("%,d", j),
                  String.format("%,d", threshold),
                  String.format("%,d", stopWatch.getTime()),
                  String.format("%,d", stopWatch.getTime() / threshold)
              );
              stopWatch.reset();
              stopWatch.start();
            }
          }
          log.info("status=end");
        } catch (Exception e) {
          log.error("status=failed, msg={}", e.getMessage(), e);
        }
      });
    }
    executorService.shutdown();
    executorService.awaitTermination(10, TimeUnit.MINUTES);
    log.info("status=producer-finished, records={}", recordsSent.get());
    Thread.sleep(15_000);
    Threads.threadDump();
  }


}
