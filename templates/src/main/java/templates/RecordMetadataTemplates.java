package templates;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

public class RecordMetadataTemplates {
  private RecordMetadataTemplates() {
  }

  public static RecordMetadata build() {
    return new RecordMetadata(
        new TopicPartition("fruit", 1),
        -1L,
        -1L,
        -1L,
        Long.valueOf(-1),
        0,
        0
    );
  }
}
