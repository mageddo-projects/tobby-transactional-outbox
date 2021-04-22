package com.mageddo.tobby.replicator;

import java.sql.Connection;

public interface StreamingIterator {
  int iterate(Connection readConn);
}
