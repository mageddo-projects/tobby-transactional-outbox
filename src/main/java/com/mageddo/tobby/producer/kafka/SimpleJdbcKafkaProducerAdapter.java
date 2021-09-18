package com.mageddo.tobby.producer.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.mageddo.tobby.internal.utils.SyncFuture;

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
import org.apache.kafka.common.serialization.Serializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SimpleJdbcKafkaProducerAdapter<K, V> implements Producer<K, V> {

  private final JdbcKafkaProducer<K, V> jdbcKafkaProducer;

  public SimpleJdbcKafkaProducerAdapter(
      Serializer<K> keySerializer, Serializer<V> valueSerializer, com.mageddo.tobby.producer.Producer producer
  ) {
    this(new JdbcKafkaProducer<>(
        producer, keySerializer, valueSerializer
    ));
  }

  public SimpleJdbcKafkaProducerAdapter(
      JdbcKafkaProducer<K, V> jdbcKafkaProducer
  ) {
    this.jdbcKafkaProducer = jdbcKafkaProducer;
  }

  @Override
  public void initTransactions() {
    this.transactionUnsupportedError();
  }

  @Override
  public void beginTransaction() throws ProducerFencedException {
    this.transactionUnsupportedError();
  }

  @Override
  public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
      String consumerGroupId)
      throws ProducerFencedException {
    this.transactionUnsupportedError();
  }

  public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
      org.apache.kafka.clients.consumer.ConsumerGroupMetadata groupMetadata)
      throws ProducerFencedException {
    this.transactionUnsupportedError();
  }

  @Override
  public void commitTransaction() throws ProducerFencedException {
    this.transactionUnsupportedError();
  }

  @Override
  public void abortTransaction() throws ProducerFencedException {
    this.transactionUnsupportedError();
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
    final RecordMetadata produced = this.save(record);
    return this.buildPromise(produced);
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
    final RecordMetadata metadata = this.save(record);
    try {
      callback.onCompletion(metadata, null);
    } catch (Throwable e){
      log.warn("status=callbackFailed, msg={}", e.getMessage(), e);
    }
    return this.buildPromise(metadata);
  }

  @Override
  public void flush() {
    // ( ͡° ͜ʖ ͡°)
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    return Collections.EMPTY_LIST;
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    return Collections.EMPTY_MAP;
  }

  @Override
  public void close() {
    // the silence is golden
  }

  public void close(long timeout, TimeUnit unit) {
    // the silence is golden
  }

  public void close(Duration timeout) {
    // the silence is golden
  }

  private void transactionUnsupportedError() {
    throw new UnsupportedOperationException(
        "This is a jdbc producer, no kafka transactions are necessary"
    );
  }

  private Future<RecordMetadata> buildPromise(RecordMetadata metadata) {
    return new SyncFuture<>(metadata);
  }

  private RecordMetadata save(ProducerRecord<K, V> record) {
    return this.jdbcKafkaProducer.send(record);
  }

}
