import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.mageddo.tobby.Tobby;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import templates.KafkaProducerRecordTemplates;
import testing.DBMigration;

public class Main {
  public static void main2(String[] args) throws ExecutionException, InterruptedException {

    final var kafkaProducer = new KafkaProducer<String, byte[]>(
        Map.of(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
        ),
        new StringSerializer(),
        new ByteArraySerializer()
    );

    System.out.println(kafkaProducer.send(KafkaProducerRecordTemplates.coconut()).get());;
  }

  public static void main(String[] args) {

    final var tobby = Tobby.build(DBMigration.migratePostgres());
    final var producer = tobby.jdbcProducerAdapter(
        StringSerializer.class, ByteArraySerializer.class
    );

    final var metadata = producer.send(KafkaProducerRecordTemplates.coconut());

    System.out.println(metadata);

  }
}
