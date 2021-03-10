package com.mageddo.tobby.replicator;

import com.mageddo.tobby.ProducedRecord;

public interface Replicator {
  void send(ProducedRecord record);

  void flush();
}
