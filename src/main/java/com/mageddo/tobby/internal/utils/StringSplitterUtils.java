package com.mageddo.tobby.internal.utils;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class StringSplitterUtils {

  private static final String SEPARATOR = "\\|";

  public static List<UUID> fromStrToUuidList(String str) {
    return fromStrToList(str, UUID::fromString);
  }

  public static List<UUID> fromStrToList(String str, Function<String, UUID> mapper) {
    if (StringUtils.isBlank(str)) {
      return Collections.EMPTY_LIST;
    }
    return Stream
        .of(str.split(SEPARATOR))
        .map(mapper)
        .collect(Collectors.toList());
  }
}
