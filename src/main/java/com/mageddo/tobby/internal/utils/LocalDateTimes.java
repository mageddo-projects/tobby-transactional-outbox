package com.mageddo.tobby.internal.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Utils for dates, UTC is the used timezone
 */
public class LocalDateTimes {

  public static final ZoneId UTC_ZONED_ID = ZoneId.of("UTC");

  public static LocalDateTime now() {
    return ZonedDateTime
        .now(UTC_ZONED_ID)
        .toLocalDateTime();
  }

  public static LocalDateTime daysAgo(int daysAgo) {
    return now().minusDays(daysAgo);
  }

  public static LocalDateTime daysInTheFuture(int daysToSum) {
    return now().plusDays(daysToSum);
  }

  public static LocalDateTime minutesAgo(int minutes) {
    return now().minusMinutes(minutes);
  }
}
