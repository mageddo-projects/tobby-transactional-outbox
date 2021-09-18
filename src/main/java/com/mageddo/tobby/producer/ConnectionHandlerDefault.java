package com.mageddo.tobby.producer;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ConnectionHandlerDefault implements ConnectionHandler {

  private final Connection connection;
  private final List<Callback> afterCommitCallbacks = new ArrayList<>();

  public ConnectionHandlerDefault(Connection connection) {
    this.connection = connection;
  }

  @Override
  public Connection connection() {
    return this.connection;
  }

  @Override
  public ConnectionHandler afterCommit(Callback callback) {
    this.afterCommitCallbacks.add(callback);
    return this;
  }

  @Override
  public List<Callback> afterCommitCallbacks() {
    return Collections.unmodifiableList(this.afterCommitCallbacks);
  }
}
