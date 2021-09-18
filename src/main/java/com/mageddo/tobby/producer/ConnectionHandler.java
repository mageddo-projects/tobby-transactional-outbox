package com.mageddo.tobby.producer;

import java.sql.Connection;
import java.util.List;

@Deprecated
public interface ConnectionHandler {

  Connection connection();

  ConnectionHandler afterCommit(Callback callback);

  List<Callback> afterCommitCallbacks();

  interface Callback {
    void call();
  }

  static ConnectionHandler wrap(Connection connection){
    return new ConnectionHandlerDefault(connection);
  }
}
