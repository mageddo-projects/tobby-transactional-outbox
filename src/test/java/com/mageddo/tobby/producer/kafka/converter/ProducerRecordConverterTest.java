package com.mageddo.tobby.producer.kafka.converter;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import templates.KafkaProducerRecordTemplates;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

  @Test
  void mustSaveProducedRecordWithCustomizedEventIdAndRemoveTobbyHeaders() {
    // arrange
    final var eventId = UUID.fromString("51b466e3-bb4b-47ad-a6d8-7d1324c2bef6");
    final var record = KafkaProducerRecordTemplates.withCustomEventID(eventId);

    // act
    final var result = ProducerRecordConverter.of(
        new StringSerializer(), new StringSerializer(), record
    );

    // assert
    assertEquals(eventId, result.getId());
    assertTrue(
        result
            .getHeaders()
            .isEmpty(),
        result.getHeaders()
            .toString()
    );
  }

  @Test
  void mustSaveProducedRecordWithCustomizedEventIdAndRemoveTobbyHeadersButKeepOthers() {
    // arrange
    final var eventId = UUID.fromString("51b466e3-bb4b-47ad-a6d8-7d1324c2bef6");
    final var record = KafkaProducerRecordTemplates.withCustomEventIDAndAnotherHeader(eventId);

    // act
    final var result = ProducerRecordConverter.of(
        new StringSerializer(), new StringSerializer(), record
    );

    // assert
    assertEquals(eventId, result.getId());
    assertEquals("[{X-KEY=[(X-KEY, 1)]}]", String.valueOf(result.getHeaders()));
  }

  @Test
  void mustSaveProducedRecordRandomEventId() {
    // arrange
    final var record = KafkaProducerRecordTemplates.mango();

    // act
    final var firstId = ProducerRecordConverter
        .of(
            new StringSerializer(), new StringSerializer(), record
        )
        .getId();

    final var secondId = ProducerRecordConverter
        .of(
            new StringSerializer(), new StringSerializer(), record
        )
        .getId();

    // assert
    assertNotEquals(firstId, secondId);
  }

}
