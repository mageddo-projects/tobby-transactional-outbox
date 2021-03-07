package com.mageddo.tobby;

public class ProducerRecord {

  private final String topic;
  private final Integer partition;
  private final byte[] key;
  private final byte[] value;
  private final Headers headers;

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
}
