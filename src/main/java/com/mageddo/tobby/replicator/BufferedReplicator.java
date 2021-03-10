package com.mageddo.tobby.replicator;

import java.sql.Connection;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.mageddo.tobby.ParameterDAO;
import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.internal.utils.StopWatch;

import org.apache.kafka.clients.producer.Producer;

import static com.mageddo.tobby.Parameter.LAST_PROCESSED_TIMESTAMP;
import static com.mageddo.tobby.producer.kafka.converter.ProducedRecordConverter.toKafkaProducerRecord;
import static org.apache.kafka.common.requests.DeleteAclsResponse.log;

public class BufferedReplicator {

  private LocalDateTime lastRecordCreatedAt;
  private final Connection connection;
  private final RecordDAO recordDAO;
  private final ParameterDAO parameterDAO;
  private final Producer<byte[], byte[]> producer;
  private final List<ProducedRecord> buffer;
  private final int bufferSize;
  private final int wave;

  public BufferedReplicator(
      Connection connection, RecordDAO recordDAO, ParameterDAO parameterDAO,
      Producer<byte[], byte[]> producer, int wave
  ) {
    this.connection = connection;
    this.recordDAO = recordDAO;
    this.parameterDAO = parameterDAO;
    this.producer = producer;
    this.wave = wave;
    this.bufferSize = 32_000;
    this.buffer = new ArrayList<>(this.bufferSize);
  }


  public void send(ProducedRecord record) {
    //    final boolean autoCommit = connection.getAutoCommit();
//    connection.setAutoCommit(false);
//    connection.setAutoCommit(true);
//    connection.setAutoCommit(autoCommit);
    // FIXME criar um lote via codigo e commitar toda vez que esse lote atingir o limite,
    // fazer os updates e inserts em outra conexao para nao dar commit na que esta fazendo o select,
    // senao ela aborta o streaming
    if (this.buffer.size() < this.bufferSize) {
      this.recordDAO.acquire(this.connection, record.getId());
      this.buffer.add(record);
    }
    this.send();
  }

  public void flush() {
    this.send();
  }

  private void send() {
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
        for (RecordSend future : futures) {
          future
              .getFuture()
              .get();
          this.lastRecordCreatedAt = future
              .getProducedRecord()
              .getCreatedAt();
        }

        final long produceTime = recordStopWatch.getSplitTime();
        recordStopWatch.split();

        if (this.bufferSize > 1000) {
          log.info(
              "wave={}, status=recordsSent, acquire={}, produce={}, record={}",
              this.wave,
              StopWatch.display(-1),
              StopWatch.display(produceTime),
              recordStopWatch.getTime()
          );
        }
        break;
      } catch (InterruptedException | ExecutionException e) {
        log.warn("wave={}, status=failed-to-post-to-kafka, msg={}", this.wave, e.getMessage(), e);
      }
    }
  }

  public void updateLastSent() {
    this.updateLastUpdate(this.connection, this.lastRecordCreatedAt);
  }

  private void updateLastUpdate(Connection connection, LocalDateTime createdAt) {
    if (createdAt == null) {
      if (log.isDebugEnabled()) {
        log.debug("status=no-date-to-update");
      }
      return;
    }
    this.parameterDAO.insertOrUpdate(connection, LAST_PROCESSED_TIMESTAMP, createdAt);
  }

  public int size() {
    return this.buffer.size();
  }
}
