package com.mageddo.tobby.producer;

import java.sql.Connection;

import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.ProducerRecord;

public interface Producer {

  ProducedRecord send(ProducerRecord record);

  ProducedRecord send(Connection connection, ProducerRecord record);
}
