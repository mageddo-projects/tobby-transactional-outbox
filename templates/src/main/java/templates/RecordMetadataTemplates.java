package templates;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Random;

public class RecordMetadataTemplates {

  private static final Random RANDOM = new Random();

  private RecordMetadataTemplates() {
  }

  public static RecordMetadata timestampBasedRecordMetadata(){
    final long offset = System.nanoTime();
    return  new RecordMetadata(
        new TopicPartition("fruit", RANDOM.nextInt(20)),
        offset,
        offset,
        System.currentTimeMillis(),
        Long.valueOf(System.nanoTime()),
        0,
        0
    );
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
