package com.mageddo.tobby;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class ProducerRecord {

  private String topic;
  private Integer partition;
  private byte[] key;
  private byte[] value;
  private Headers headers;

  public ProducerRecord(String topic, byte[] value) {
    this(topic, null, value);
  }

  public ProducerRecord(String topic, byte[] key, byte[] value) {
    this(topic, null, key, value);
  }

  public ProducerRecord(String topic, Integer partition, byte[] key, byte[] value) {
    this(topic, partition, key, value, null);
  }

  public ProducerRecord(
      String topic, Integer partition, byte[] key, byte[] value, Headers headers
  ) {
    this.topic = topic;
    this.partition = partition;
    this.key = key;
    this.value = value;
    this.headers = headers;
  }

}
