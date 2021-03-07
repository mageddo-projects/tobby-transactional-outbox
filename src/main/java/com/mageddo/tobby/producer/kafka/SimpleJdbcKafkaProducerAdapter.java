package com.mageddo.tobby.producer.kafka;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.mageddo.tobby.RecordDAOUniversal;

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

public class SimpleJdbcKafkaProducerAdapter<K, V> implements Producer<K, V> {

  private final JdbcKafkaProducer<K, V> jdbcKafkaProducer;
  private final ExecutorService executorService;

  public SimpleJdbcKafkaProducerAdapter(
      DataSource dataSource, Serializer<K> keySerializer, Serializer<V> valueSerializer
  ) {
    this(new JdbcKafkaProducer<>(new RecordDAOUniversal(dataSource), keySerializer, valueSerializer));
  }

  public SimpleJdbcKafkaProducerAdapter(JdbcKafkaProducer<K, V> jdbcKafkaProducer) {
    this(Executors.newFixedThreadPool(5), jdbcKafkaProducer);
  }

  public SimpleJdbcKafkaProducerAdapter(
      ExecutorService executorService, JdbcKafkaProducer<K, V> jdbcKafkaProducer
  ) {
    this.executorService = executorService;
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
    return this.executorService.submit(() -> {
      callback.onCompletion(metadata, null);
      return metadata;
    });
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
    this.executorService.shutdown();
  }

  @Override
  public void close(long timeout, TimeUnit unit) {
    try {
      this.executorService.awaitTermination(timeout, unit);
    } catch (InterruptedException e) {
    }
    this.executorService.shutdownNow();
  }

  private void transactionUnsupportedError() {
    throw new UnsupportedOperationException(
        "This is a jdbc producer, no kafka transactions are necessary"
    );
  }

  private Future<RecordMetadata> buildPromise(RecordMetadata metadata) {
    return this.executorService.submit(() -> metadata);
  }

  private RecordMetadata save(ProducerRecord<K, V> record) {
    return this.jdbcKafkaProducer.send(record);
  }

}
