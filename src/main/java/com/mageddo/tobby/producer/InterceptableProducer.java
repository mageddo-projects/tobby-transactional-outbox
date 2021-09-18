package com.mageddo.tobby.producer;

import com.mageddo.tobby.ProducedRecord;
import com.mageddo.tobby.ProducerRecord;

public interface InterceptableProducer {
  ProducedRecord send(ProducerRecord record);

  ProducedRecord send(ConnectionHandler handler, ProducerRecord record);
}
