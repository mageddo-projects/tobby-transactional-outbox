package com.mageddo.tobby.producer.kafka.converter;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import templates.KafkaProducerRecordTemplates;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ProducerRecordConverterTest {


  @Test
  void mustConvertFromKafkaProducerRecordToProducerRecord() {

    // arrange
    final var record = KafkaProducerRecordTemplates.coconut();

    // act
    final var result = ProducerRecordConverter.of(
        new StringSerializer(), new ByteArraySerializer(), record
    );

    // assert
    assertNotNull(record);
    assertArrayEquals(
        record
            .key()
            .getBytes(),
        result.getKey()
    );
    assertArrayEquals(record.value(), result.getValue());
    assertEquals(record.partition(), result.getPartition());
    assertEquals(record.topic(), result.getTopic());
    assertEquals(
        record.headers()
            .toArray().length,
        result.getHeaders()
            .asList()
            .size()
    );
    final var headersArr = record
        .headers()
        .toArray();
    for (int i = 0; i < headersArr.length; i++) {
      final var expected = headersArr[i];
      final var actual = result.getHeaders()
          .asList()
          .get(i);
      assertEquals(expected.key(), actual.getKey());
      assertArrayEquals(expected.value(), actual.getValue());
    }
  }

}
