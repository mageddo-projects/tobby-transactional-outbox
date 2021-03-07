package com.mageddo.tobby;

import java.util.Collections;
import java.util.List;

public class Headers {

  private final List<Header> headers;

  public Headers() {
    this.headers = Collections.emptyList();
  }

  public Headers(List<Header> headers) {
    this.headers = headers;
  }

  public List<Header> getHeaders() {
    return headers;
  }
}
