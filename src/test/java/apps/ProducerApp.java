package apps;

import java.util.concurrent.Executors;

import com.mageddo.tobby.Tobby;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import templates.KafkaProducerRecordTemplates;
import testing.DBMigration;

public class ProducerApp {

  public static void main(String[] args) throws InterruptedException {
    DBMigration.migratePostgres();
    final var tobby = Tobby.build(ReplicatorApp.dataSource(60));
    final var producer = tobby.kafkaProducer(
        StringSerializer.class, ByteArraySerializer.class
    );

    final var executorService = Executors.newFixedThreadPool(50);
    for (int i = 0; i < 50; i++) {
      executorService.submit(() -> {
        while (true){
          producer.send(KafkaProducerRecordTemplates.coconut());
        }
      });
    }
    Thread.currentThread().join();
  }
}
