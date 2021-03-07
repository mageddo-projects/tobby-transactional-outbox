package com.mageddo.tobby.adapters.kafka;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.mageddo.tobby.Headers;
import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.ProducerRecordReq;
import com.mageddo.tobby.RecordDAO;
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

public class SimpleJdbcProducer<K, V> implements Producer<K, V> {

  private final ExecutorService executorService;
  private final RecordDAO recordDAO;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;

  public SimpleJdbcProducer(
      DataSource dataSource, Serializer<K> keySerializer, Serializer<V> valueSerializer
  ) {
    this(new RecordDAOUniversal(dataSource), keySerializer, valueSerializer);
  }

  public SimpleJdbcProducer(RecordDAO recordDAO, Serializer<K> keySerializer,
      Serializer<V> valueSerializer) {
    this(Executors.newFixedThreadPool(5), recordDAO, keySerializer, valueSerializer);
  }

  public SimpleJdbcProducer(ExecutorService executorService, RecordDAO recordDAO,
      Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    this.executorService = executorService;
    this.recordDAO = recordDAO;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
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
    final ProducedRecord produced = this.save(record);
    return this.buildPromise(produced);
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
    final ProducedRecord produced = this.save(record);
    return this.executorService.submit(() -> {
      final RecordMetadata metadata = this.buildMetadata(produced);
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

  private Future<RecordMetadata> buildPromise(ProducedRecord record) {
    return this.executorService.submit(() -> this.buildMetadata(record));
  }

  private RecordMetadata buildMetadata(ProducedRecord record) {
    return new RecordMetadata(
        new TopicPartition(record.getTopic(), record.getPartition()),
        0L,
        (long) record.getId(),
        toMillis(record),
        this.digest(record),
        calcSize(record.getKey()),
        calcSize(record.getValue())
    );
  }

  private long toMillis(ProducedRecord record) {
    return Timestamp.valueOf(record.getCreatedAt())
        .toInstant()
        .toEpochMilli();
  }

  private int calcSize(byte[] record) {
    return record == null ? 0 : record.length;
  }

  private Long digest(ProducedRecord record) {
    throw new UnsupportedOperationException();
  }

  private ProducedRecord save(ProducerRecord<K, V> record) {
    return this.recordDAO.save(this.toRecord(record));
  }

  private ProducerRecordReq toRecord(ProducerRecord<K, V> record) {
    return new ProducerRecordReq(
        record.topic(), record.partition(),
        this.keySerializer.serialize(record.topic(), record.key()),
        this.valueSerializer.serialize(record.topic(), record.value()),
        this.encodeHeaders(record)
    );
  }

  private Headers encodeHeaders(ProducerRecord<K, V> record) {
    throw new UnsupportedOperationException();
  }
}
