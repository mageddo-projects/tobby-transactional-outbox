package com.mageddo.tobby.producer;

import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.ProducerRecord;

import java.sql.Connection;

public interface Producer {
  ProducedRecord send(ProducerRecord record);

  ProducedRecord send(Connection connection, ProducerRecord record);
}
