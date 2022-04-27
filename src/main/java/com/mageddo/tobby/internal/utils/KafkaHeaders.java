package com.mageddo.tobby.internal.utils;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import static com.mageddo.tobby.Headers.TOBBY_EVENT_ID;

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

  public static Header of(String k, String v) {
    return of(k, v.getBytes());
  }

  public static Header of(String k, byte[] v) {
    return new RecordHeader(k, v);
  }

  public static Headers headersOf(String k, String v) {
    return headersOf(of(k, v));
  }

  public static Headers headersOf(String k, byte[] v) {
    return headersOf(of(k, v));
  }

  public static Headers headersOf(Header header) {
    return new RecordHeaders(new Header[]{header});
  }

  public static Headers of(Map<String, byte[]> map) {
    return new RecordHeaders(
        map
            .entrySet()
            .stream()
            .map(it -> of(it.getKey(), it.getValue()))
            .collect(Collectors.toList())
    );
  }

  public static Headers withEventId(UUID eventId) {
    return headersOf(
        TOBBY_EVENT_ID,
        eventId
            .toString()
            .getBytes()
    );
  }
}
