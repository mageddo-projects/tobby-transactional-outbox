package com.mageddo.tobby.internal.utils;

public class StringUtils {

  public static final String EMPTY = "";

  private StringUtils() {
  }

  public static boolean isBlank(String str) {
    return str == null || str.equals("");
  }

  /**
   * <p>Returns either the passed in String,
   * or if the String is {@code null}, an empty String ("").</p>
   *
   * <pre>
   * StringUtils.defaultString(null)  = ""
   * StringUtils.defaultString("")    = ""
   * StringUtils.defaultString("bat") = "bat"
   * </pre>
   *
   * @see String#valueOf(Object)
   * @param str  the String to check, may be null
   * @return the passed in String, or the empty String if it
   *  was {@code null}
   */
  public static String defaultString(final String str) {
    return defaultString(str, EMPTY);
  }

  /**
   * <p>Returns either the passed in String, or if the String is
   * {@code null}, the value of {@code defaultStr}.</p>
   *
   * <pre>
   * StringUtils.defaultString(null, "NULL")  = "NULL"
   * StringUtils.defaultString("", "NULL")    = ""
   * StringUtils.defaultString("bat", "NULL") = "bat"
   * </pre>
   *
   * @see String#valueOf(Object)
   * @param str  the String to check, may be null
   * @param defaultStr  the default String to return
   *  if the input is {@code null}, may be null
   * @return the passed in String, or the default if it was {@code null}
   */
  public static String defaultString(final String str, final String defaultStr) {
    return str == null ? defaultStr : str;
  }
}
