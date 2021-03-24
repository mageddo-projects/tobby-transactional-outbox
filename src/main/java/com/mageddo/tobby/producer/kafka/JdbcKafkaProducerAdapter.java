package com.mageddo.tobby.producer.kafka;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;

public class JdbcKafkaProducerAdapter<K, V> implements Producer<K, V> {

  private final SimpleJdbcKafkaProducerAdapter<K, V> jdbcDelegate;
  private final Producer<K, V> kafkaDelegate;

  public JdbcKafkaProducerAdapter(
      Producer<K, V> kafkaDelegate, SimpleJdbcKafkaProducerAdapter<K, V> jdbcDelegate
  ) {
    this.kafkaDelegate = kafkaDelegate;
    this.jdbcDelegate = jdbcDelegate;
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
    return this.jdbcDelegate.send(record);
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
    return this.jdbcDelegate.send(record, callback);
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    return this.kafkaDelegate.partitionsFor(topic);
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    return this.kafkaDelegate.metrics();
  }

  @Override
  public void flush() {
    this.kafkaDelegate.flush();
  }

  @Override
  public void close() {
    this.jdbcDelegate.close();
    this.kafkaDelegate.close();
  }

  public void close(long timeout, TimeUnit unit) {
    this.jdbcDelegate.close(timeout, unit);
    this.kafkaDelegate.close(timeout, unit);
  }

  public void close(Duration timeout) {
    this.close(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void initTransactions() {
    this.jdbcDelegate.initTransactions();
  }

  @Override
  public void beginTransaction() throws ProducerFencedException {
    this.jdbcDelegate.initTransactions();
  }

  @Override
  public void sendOffsetsToTransaction(
      Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId
  ) {
    this.jdbcDelegate.initTransactions();
  }

  public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata)
      throws ProducerFencedException {
    this.jdbcDelegate.sendOffsetsToTransaction(offsets, groupMetadata);
  }

  @Override
  public void commitTransaction() throws ProducerFencedException {
    this.jdbcDelegate.initTransactions();
  }

  @Override
  public void abortTransaction() throws ProducerFencedException {
    this.jdbcDelegate.initTransactions();
  }

}
