package com.mageddo.tobby.internal.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Base64 {

  public static String encodeToString(byte[] data) {
    if (data == null) {
      return null;
    }
    return java.util.Base64.getEncoder()
        .encodeToString(data);
  }

  public static byte[] decode(String data) {
    if (StringUtils.isBlank(data)) {
      return null;
    }
    return java.util.Base64.getDecoder()
        .decode(data);
  }
}
