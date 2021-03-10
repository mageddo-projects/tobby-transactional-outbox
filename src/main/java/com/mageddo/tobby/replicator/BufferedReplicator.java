package com.mageddo.tobby.replicator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.internal.utils.StopWatch;

import org.apache.kafka.clients.producer.Producer;

import lombok.extern.slf4j.Slf4j;

import static com.mageddo.tobby.producer.kafka.converter.ProducedRecordConverter.toKafkaProducerRecord;

@Slf4j
public class BufferedReplicator implements Replicator {

  private final Producer<byte[], byte[]> producer;
  private final List<ProducedRecord> buffer;
  private final int bufferSize;
  private final int wave;
  private final StopWatch stopWatch;

  public BufferedReplicator(Producer<byte[], byte[]> producer, int wave) {
    this.producer = producer;
    this.wave = wave;
    this.bufferSize = 50_000;
    this.buffer = new ArrayList<>(this.bufferSize);
    this.stopWatch = new StopWatch();
  }

  @Override
  public boolean send(ProducedRecord record) {
    this.buffer.add(record);
    if (this.buffer.size() < this.bufferSize) {
      if (log.isTraceEnabled()) {
        log.trace("status=addToBuffer, id={}", record.getId());
      }
      return false;
    }
    return true;
  }

  @Override
  public void flush() {
    long elapsedTimeSinceLastFlush = this.getTimeSinceLastFlush();
    this.stopWatch.reset();
    if (log.isDebugEnabled()) {
      log.debug("status=sending, wave={}, records={}", this.wave, this.buffer.size());
    }
    while (true) {
      try {
        final StopWatch recordStopWatch = StopWatch.createStarted();
        final List<RecordSend> futures = new ArrayList<>();
        for (ProducedRecord producedRecord : this.buffer) {
          futures.add(new RecordSend(
              producedRecord,
              this.producer.send(toKafkaProducerRecord(producedRecord))
          ));
        }
        if (log.isDebugEnabled()) {
          log.debug(
              "status=kafkaSendScheduled, wave={}, records={}, time={}",
              this.wave, this.buffer.size(), recordStopWatch.getTime()
          );
        }
        for (RecordSend future : futures) {
          future
              .getFuture()
              .get();
        }
        final long produceTime = recordStopWatch.getSplitTime();
        recordStopWatch.split();
        if (log.isDebugEnabled()) {
          log.debug(
              "status=kafkaSendFlushed, wave={}, records={}, time={}",
              this.wave, this.buffer.size(), produceTime
          );
        }

        if (this.bufferSize > 1000) {
          log.info(
              "wave={}, quantity={}, status=kafkaSendFlushed, timeSinceLastFlush={}, produceTime={}, recordsTime={}",
              this.wave,
              this.buffer.size(),
              StopWatch.display(elapsedTimeSinceLastFlush),
              StopWatch.display(produceTime),
              recordStopWatch.getTime()
          );
        }
        this.buffer.clear();
        break;
      } catch (InterruptedException | ExecutionException e) {
        log.warn("wave={}, status=failed-to-post-to-kafka, msg={}", this.wave, e.getMessage(), e);
      }
    }
  }

  private long getTimeSinceLastFlush() {
    if (!this.stopWatch.isStarted()) {
      return 0;
    }
    final long time = this.stopWatch.getTime();
    this.stopWatch.reset();
    return time;
  }

  public int size() {
    return this.buffer.size();
  }
}
