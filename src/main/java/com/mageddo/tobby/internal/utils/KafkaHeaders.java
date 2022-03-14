package com.mageddo.tobby.internal.utils;

import java.util.Optional;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaHeaders {

  public static byte[] lastHeaderOrNull(Headers headers, String key) {
    final Header header = headers.lastHeader(key);
    if (header == null) {
      return null;
    }
    return header.value();
  }

  public static String lastHeaderAsTextOrNull(Headers headers, String key) {
    return Optional
        .ofNullable(lastHeaderOrNull(headers, key))
        .map(String::new)
        .orElse(null);
  }

}
