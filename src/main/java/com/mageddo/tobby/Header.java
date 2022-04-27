package com.mageddo.tobby;

import java.util.Arrays;

public class Header {

  private final String key;
  private final byte[] value;

  public Header(String key, byte[] value) {
    this.key = key;
    this.value = value;
  }

  public static Header of(String key, String value) {
    return of(key, value.getBytes());
  }

  public static Header of(String key, byte[] value) {
    return new Header(key, value);
  }

  public String getKey() {
    return key;
  }

  public byte[] getValue() {
    return value;
  }

  @Override
  public String toString() {
    return String.format("(%s, %s)", this.key, new String(this.value));
  }
}
