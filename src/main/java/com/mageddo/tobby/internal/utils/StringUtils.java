package com.mageddo.tobby.internal.utils;

public class StringUtils {
  private StringUtils() {
  }

  public static boolean isBlank(String str) {
    return str == null || str.equals("");
  }
}
