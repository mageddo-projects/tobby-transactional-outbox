package templates;

import java.util.List;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

public class KafkaProducerRecordTemplates {
  public static ProducerRecord<String, byte[]> coconut() {
    return new ProducerRecord<>(
        "fruit",
        0,
        "greenFruits",
        "coconuts".getBytes(),
        List.of(new RecordHeader("version", "1".getBytes()))
    );
  }

  public static ProducerRecord<String, String> mango() {
    return new ProducerRecord<>("fruit", "Mango");
  }
}
