package templates;

import com.mageddo.tobby.Headers;
import com.mageddo.tobby.ProducerRecord;
import com.mageddo.tobby.producer.kafka.converter.ProducerRecordConverter;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ProducerRecordTemplates {

  public static ProducerRecord strawberry() {
    return new ProducerRecord("fruit", "Strawberry".getBytes(), "Strawberry".getBytes());
  }

  public static ProducerRecord banana() {
    return new ProducerRecord("fruit", null, null);
  }

  public static ProducerRecord strawberryWithHeaders() {
    return strawberry()
        .toBuilder()
        .headers(Headers.of("version", "1".getBytes()))
        .build();
  }

  public static ProducerRecord coconut() {
    return ProducerRecordConverter.of(
        new StringSerializer(), new ByteArraySerializer(), KafkaProducerRecordTemplates.coconut()
    );
  }

  public static ProducerRecord grape() {
    return new ProducerRecord(
        "fruit",
        "some key".getBytes(),
        "Grape".getBytes()
    );
  }
}
