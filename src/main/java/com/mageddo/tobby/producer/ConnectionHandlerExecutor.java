package com.mageddo.tobby.producer;

import com.mageddo.tobby.producer.ConnectionHandler.Callback;

public class ConnectionHandlerExecutor {
  public static void executeAfterCommitCallbacks(ConnectionHandler handler) {
    handler
        .afterCommitCallbacks()
        .forEach(Callback::call);
  }
}
