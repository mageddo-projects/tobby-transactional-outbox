package com.mageddo.tobby;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import java.util.UUID;

@Value
@Builder(toBuilder = true)
public class ProducerRecord {

  @NonNull
  private UUID id;

  @NonNull
  private String topic;
  private Integer partition;
  private byte[] key;
  private byte[] value;
  private Headers headers;

  public static ProducerRecord of(String topic, byte[] key, byte[] value) {
    return ProducerRecord.builder()
        .id(UUID.randomUUID())
        .topic(topic)
        .key(key)
        .value(value)
        .build();
  }
}
