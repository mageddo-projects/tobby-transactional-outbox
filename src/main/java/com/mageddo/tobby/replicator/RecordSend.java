package com.mageddo.tobby.replicator;

import java.util.concurrent.Future;

import com.mageddo.tobby.ProducedRecord;

import org.apache.kafka.clients.producer.RecordMetadata;

import lombok.Value;

@Value
public class RecordSend {
  private ProducedRecord producedRecord;
  private Future<RecordMetadata> future;

}
