package com.mageddo.tobby;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class Headers implements Iterable<Header> {

  public static final String TOBBY_EVENT_ID = "TTO_EID";

  private final Map<String, List<Header>> headers;

  public Headers() {
    this.headers = new LinkedHashMap<>();
  }

  public Headers(Map<String, List<Header>> headers) {
    this.headers = headers;
  }

  public Headers(List<Header> headers) {
    this();
    headers.forEach(this::add);
  }

  public static Headers of(Header... headers) {
    return new Headers(
        Arrays.stream(headers)
            .collect(Collectors.toList())
    );
  }

  public static Headers of(String key, byte[] value) {
    return of(Header.of(key, value));
  }

  public static Headers withEventId(UUID id) {
    final byte[] b = String
        .valueOf(id)
        .getBytes();
    return of(TOBBY_EVENT_ID, b);
  }

  public Headers add(String key, byte[] value) {
    return this.add(Header.of(key, value));
  }

  public Headers add(Header header) {
    if (!this.headers.containsKey(header.getKey())) {
      this.headers.put(header.getKey(), new ArrayList<>());
    }
    this.headers.get(header.getKey())
        .add(header);
    return this;
  }

  public List<Header> asList() {
    return this.headers.values()
        .stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  @Override
  public Iterator<Header> iterator() {
    return this.asList()
        .iterator();
  }

  public List<Header> get(String key) {
    return Collections.unmodifiableList(this.headers.get(key));
  }

  public Header getFirst(String key) {
    return Optional.ofNullable(this.headers.get(key))
        .map(it -> it.get(0))
        .orElse(null);
  }

  public boolean isEmpty() {
    return this.headers.isEmpty();
  }
}
