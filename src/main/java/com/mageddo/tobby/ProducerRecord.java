package com.mageddo.tobby;

import java.time.LocalDateTime;
import java.util.UUID;

import com.mageddo.tobby.internal.utils.LocalDateTimes;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

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

  @NonNull
  @Builder.Default
  private LocalDateTime createdAt = LocalDateTimes.now();

  public static ProducerRecord of(String topic, byte[] key, byte[] value) {
    return ProducerRecord.builder()
        .id(UUID.randomUUID())
        .topic(topic)
        .key(key)
        .value(value)
        .build();
  }

  /**
   * Returns a copy of the same event with new id.
   */
  public ProducerRecord copyWithNewId() {
    return this.toBuilder()
        .id(UUID.randomUUID())
        .build();
  }
}
