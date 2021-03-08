import com.mageddo.tobby.Tobby;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import templates.KafkaProducerRecordTemplates;
import testing.DBMigration;

public class Main {

  public static void main(String[] args) {

    final var tobby = Tobby.build(DBMigration.migratePostgres());
    final var producer = tobby.kafkaProducer(
        StringSerializer.class, ByteArraySerializer.class
    );

    while (true){
      final var metadata = producer.send(KafkaProducerRecordTemplates.coconut());
      System.out.println(metadata);
    }

  }
}
