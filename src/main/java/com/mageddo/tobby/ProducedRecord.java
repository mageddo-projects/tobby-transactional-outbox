package com.mageddo.tobby;

import java.time.LocalDateTime;
import java.util.UUID;

public class ProducedRecord {

  private final UUID id;

  /**
   * https://stackoverflow.com/a/37067544/2979435
   */
  private final String topic;
  private final Integer partition;
  private final byte[] key;
  private final byte[] value;
  private final Headers headers;
  private final LocalDateTime createdAt;

  public ProducedRecord(UUID id, String topic, Integer partition, byte[] key, byte[] value,
      Headers headers, LocalDateTime createdAt) {
    this.id = id;
    this.topic = topic;
    this.partition = partition;
    this.key = key;
    this.value = value;
    this.headers = headers;
    this.createdAt = createdAt;
  }

  public UUID getId() {
    return id;
  }

  public String getTopic() {
    return topic;
  }

  public Integer getPartition() {
    return partition;
  }

  public byte[] getKey() {
    return key;
  }

  public byte[] getValue() {
    return value;
  }

  public Headers getHeaders() {
    return headers;
  }

  public LocalDateTime getCreatedAt() {
    return createdAt;
  }
}
