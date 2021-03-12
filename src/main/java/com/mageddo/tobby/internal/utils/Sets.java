package com.mageddo.tobby.internal.utils;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Sets {

  public static <T> Set<T> of(T... values) {
    return Stream.of(values)
        .collect(Collectors.toSet());
  }
}
