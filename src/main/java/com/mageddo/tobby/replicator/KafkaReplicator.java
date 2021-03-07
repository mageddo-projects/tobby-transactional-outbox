package com.mageddo.tobby.replicator;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutionException;

import javax.sql.DataSource;

import com.mageddo.tobby.ParameterDAO;
import com.mageddo.tobby.ParameterDAOUniversal;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.RecordDAOHsqldb;
import com.mageddo.tobby.UncheckedSQLException;

import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.mageddo.tobby.Parameter.LAST_PROCESSED_TIMESTAMP;
import static com.mageddo.tobby.producer.kafka.converter.ProducedRecordConverter.toKafkaProducerRecord;

public class KafkaReplicator {

  private final Logger log = LoggerFactory.getLogger(getClass());
  private final RecordDAO recordDAO;
  private final ParameterDAO parameterDAO;
  private final Producer<String, byte[]> producer;
  private final DataSource dataSource;

  public KafkaReplicator(Producer<String, byte[]> producer, DataSource dataSource) {
    this(producer, dataSource, new RecordDAOHsqldb(), new ParameterDAOUniversal());
  }

  public KafkaReplicator(
      Producer<String, byte[]> producer, DataSource dataSource,
      RecordDAO recordDAO, ParameterDAO parameterDAO
  ) {
    this.recordDAO = recordDAO;
    this.parameterDAO = parameterDAO;
    this.producer = producer;
    this.dataSource = dataSource;
  }

  public void replicate() {
    while (true) {
      try (Connection connection = this.dataSource.getConnection()) {
        final boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        this.recordDAO.iterateNotProcessedRecords(connection, (record) -> {
          while (true) {
            try {
              this.producer
                  .send(toKafkaProducerRecord(record))
                  .get();
              this.updateLastUpdate(connection, record);
              break;
            } catch (InterruptedException | ExecutionException e) {
              log.warn("status=failed-to-post-to-kafka, msg={}", e.getMessage(), e);
            }
          }
        }, this.findLastUpdate(connection));
        connection.setAutoCommit(true);
        connection.setAutoCommit(autoCommit);
      } catch (SQLException e) {
        throw new UncheckedSQLException(e);
      }
    }
  }

  private int updateLastUpdate(Connection connection, com.mageddo.tobby.ProducedRecord record) {
    return this.parameterDAO.update(connection, LAST_PROCESSED_TIMESTAMP, record.getCreatedAt());
  }

  private LocalDateTime findLastUpdate(Connection connection) {
    final LocalDateTime lastUpdate = this.parameterDAO.findAsDateTime(
        connection, LAST_PROCESSED_TIMESTAMP, LocalDateTime.MIN
    );
    return lastUpdate;
  }
}
