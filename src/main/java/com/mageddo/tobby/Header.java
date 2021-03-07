package com.mageddo.tobby;

public class Header {

  private final String key;
  private final byte[] value;

  public Header(String key, byte[] value) {
    this.key = key;
    this.value = value;
  }

  public String getKey() {
    return key;
  }

  public byte[] getValue() {
    return value;
  }
}
