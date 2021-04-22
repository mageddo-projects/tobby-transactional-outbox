package com.mageddo.tobby.replicator;

import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.internal.utils.StopWatch;
import com.mageddo.tobby.internal.utils.Threads;
import com.mageddo.tobby.internal.utils.UncheckedExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;

import lombok.extern.slf4j.Slf4j;

import static com.mageddo.tobby.producer.kafka.converter.ProducedRecordConverter.toKafkaProducerRecord;

@Slf4j
@Singleton
public class BatchSender {

  private final Producer<byte[], byte[]> producer;

  @Inject
  public BatchSender(Producer<byte[], byte[]> producer) {
    this.producer = producer;
  }

  public void send(List<ProducedRecord> records) {

    if (records.isEmpty()) {
      if (log.isTraceEnabled()) {
        log.trace("status=noBuffer, records={}", records.size());
      }
      return;
    }

    if (log.isDebugEnabled()) {
      log.debug("status=sending, records={}", records.size());
    }
    while (true) {
      try {
        final StopWatch stopWatch = StopWatch.createStarted();
        final List<Future<RecordMetadata>> futures = records
            .stream()
            .map(it -> this.producer.send(toKafkaProducerRecord(it)))
            .collect(Collectors.toList());
        Threads.get(futures);
        log.debug("status=sent, time={}", stopWatch.getTime());
        break;
      } catch (UncheckedExecutionException e) {
        log.warn("status=failed-to-post-to-kafka, msg={}", e.getMessage(), e);
      }
    }
  }

}
