package com.mageddo.tobby;

import java.util.List;

import org.apache.kafka.common.header.Header;

public class Headers {

  private final List<Header> headers;

  public Headers(List<Header> headers) {
    this.headers = headers;
  }

  public List<Header> getHeaders() {
    return headers;
  }
}
