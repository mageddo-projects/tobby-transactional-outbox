package templates;

import java.util.List;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

public class ProducerRecordTemplates {
  public static ProducerRecord<String, byte[]> coconut() {
    return new ProducerRecord<>(
        "fruit",
        55,
        "greenFruits",
        "coconuts".getBytes(),
        List.of(new RecordHeader("version", "1".getBytes()))
    );
  }
}
