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


  public static ProducerRecord<String, byte[]> articles() {
    return new ProducerRecord<String, byte[]>(
        "2021-fruit-v2",
        null,
        null,
        ("{\n"
            + "  \"data\": [{\n"
            + "    \"type\": \"articles\",\n"
            + "    \"id\": \"1\",\n"
            + "    \"attributes\": {\n"
            + "      \"title\": \"JSON:API paints my bikeshed!\",\n"
            + "      \"body\": \"The shortest article. Ever.\",\n"
            + "      \"created\": \"2015-05-22T14:56:29.000Z\",\n"
            + "      \"updated\": \"2015-05-22T14:56:28.000Z\"\n"
            + "    },\n"
            + "    \"relationships\": {\n"
            + "      \"author\": {\n"
            + "        \"data\": {\"id\": \"42\", \"type\": \"people\"}\n"
            + "      }\n"
            + "    }\n"
            + "  }],\n"
            + "  \"included\": [\n"
            + "    {\n"
            + "      \"type\": \"people\",\n"
            + "      \"id\": \"42\",\n"
            + "      \"attributes\": {\n"
            + "        \"name\": \"John\",\n"
            + "        \"age\": 80,\n"
            + "        \"gender\": \"male\"\n"
            + "      }\n"
            + "    }\n"
            + "  ]\n"
            + "}").getBytes(),
        List.of(new RecordHeader("version", "1".getBytes()))
    );
  }

  public static ProducerRecord<String, String> mango() {
    return new ProducerRecord<>("fruit", "Mango");
  }
}
