package com.mageddo.tobby.replicator;

import com.mageddo.tobby.ProducedRecord;

public interface Replicator {
  boolean send(ProducedRecord record);
  void flush();
}
