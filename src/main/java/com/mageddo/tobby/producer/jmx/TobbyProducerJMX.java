package com.mageddo.tobby.producer.jmx;

import com.mageddo.db.ConnectionUtils;
import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.internal.utils.ExceptionUtils;
import com.mageddo.tobby.internal.utils.StringSplitterUtils;
import com.mageddo.tobby.producer.Producer;
import com.mageddo.tobby.producer.kafka.converter.ProducerRecordConverter;

import javax.inject.Inject;
import javax.sql.DataSource;

import java.util.stream.Collectors;

public class TobbyProducerJMX {

  private final RecordDAO recordDAO;
  private final DataSource dataSource;
  private final com.mageddo.tobby.producer.Producer producer;

  @Inject
  public TobbyProducerJMX(RecordDAO recordDAO, DataSource dataSource, Producer producer) {
    this.recordDAO = recordDAO;
    this.dataSource = dataSource;
    this.producer = producer;
  }

  public String reSend(String recordsIdsList) {
    try {
      return ConnectionUtils.runAndClose(this.dataSource.getConnection(), (connection) -> {
        return StringSplitterUtils
            .fromStrToUuidList(recordsIdsList)
            .stream()
            .map(id -> {
              return this.recordDAO.find(connection, id);
            })
            .map(record -> {
              final ProducedRecord sent = this.producer.send(ProducerRecordConverter.of(record));
              return String.format(
                  "id=%s, sentId=%s, topic=%s, key=%s",
                  record.getId(), sent.getId(), record.getTopic(), format(record.getKey())
              );
            })
            .collect(Collectors.joining("\n"));
      });
    } catch (Exception e) {
      return ExceptionUtils.getStackTrace(e);
    }
  }

  private String format(byte[] arr) {
    if (arr == null) {
      return null;
    }
    return new String(arr);
  }
}
