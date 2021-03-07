package com.mageddo.tobby.replicator;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutionException;

import com.mageddo.tobby.Parameter;
import com.mageddo.tobby.ParameterDAO;
import com.mageddo.tobby.RecordDAO;

import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.mageddo.tobby.producer.kafka.converter.ProducedRecordConverter.toKafkaProducerRecord;

public class KafkaReplicator {

  private final Logger log = LoggerFactory.getLogger(getClass());
  private final RecordDAO recordDAO;
  private final ParameterDAO parameterDAO;
  private final Producer<String, byte[]> producer;

  public KafkaReplicator(RecordDAO recordDAO, ParameterDAO parameterDAO, Producer<String, byte[]> producer) {
    this.recordDAO = recordDAO;
    this.parameterDAO = parameterDAO;
    this.producer = producer;
  }

  public void replicate() {
    while (true) {
      final LocalDateTime lastUpdate = parameterDAO.findAsDateTime(Parameter.LAST_PROCESSED_TIMESTAMP);
      this.recordDAO.iterateNotProcessedRecords((record) -> {
        while (true) {
          try {
            this.producer
                .send(toKafkaProducerRecord(record))
                .get();
            this.parameterDAO.update(Parameter.LAST_PROCESSED_TIMESTAMP, record.getCreatedAt());
            break;
          } catch (InterruptedException | ExecutionException e) {
            log.warn("status=failed-to-post-to-kafka, msg={}", e.getMessage(), e);
          }
        }
      }, lastUpdate);
    }
  }
}
