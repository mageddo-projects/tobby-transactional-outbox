package com.mageddo.tobby.producer;

import javax.sql.DataSource;

import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.ProducerRecord;
import com.mageddo.tobby.RecordDAO;
import com.mageddo.tobby.RecordDAOUniversal;

public class Producer {

  private final RecordDAO recordDAO;

  public Producer(DataSource dataSource) {
    this(new RecordDAOUniversal(dataSource));
  }

  public Producer(RecordDAO recordDAO) {
    this.recordDAO = recordDAO;
  }

  public ProducedRecord send(ProducerRecord record) {
    return this.recordDAO.save(record);
  }
}
