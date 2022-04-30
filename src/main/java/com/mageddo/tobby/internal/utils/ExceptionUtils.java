package com.mageddo.tobby.internal.utils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Copied from apache commons lang
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ExceptionUtils {
  /**
   * Gets a short message summarising the root cause exception.
   * <p>
   * The message returned is of the form
   * {ClassNameWithoutPackage}: {ThrowableMessage}
   *
   * @param th  the throwable to get a message for, null returns empty string
   * @return the message, non-null
   * @since 2.2
   */
  public static String getRootCauseMessage(final Throwable th) {
    Throwable root = getRootCause(th);
    root = root == null ? th : root;
    return getMessage(root);
  }

  /**
   * <p>Introspects the <code>Throwable</code> to obtain the root cause.</p>
   *
   * <p>This method walks through the exception chain to the last element,
   * "root" of the tree, using {@link Throwable#getCause()}, and
   * returns that exception.</p>
   *
   * <p>From version 2.2, this method handles recursive cause structures
   * that might otherwise cause infinite loops. If the throwable parameter
   * has a cause of itself, then null will be returned. If the throwable
   * parameter cause chain loops, the last element in the chain before the
   * loop is returned.</p>
   *
   * @param throwable  the throwable to get the root cause for, may be null
   * @return the root cause of the <code>Throwable</code>,
   *  <code>null</code> if null throwable input
   */
  public static Throwable getRootCause(final Throwable throwable) {
    final List<Throwable> list = getThrowableList(throwable);
    return list.isEmpty() ? null : list.get(list.size() - 1);
  }

  /**
   * <p>Returns the list of <code>Throwable</code> objects in the
   * exception chain.</p>
   *
   * <p>A throwable without cause will return a list containing
   * one element - the input throwable.
   * A throwable with one cause will return a list containing
   * two elements. - the input throwable and the cause throwable.
   * A <code>null</code> throwable will return a list of size zero.</p>
   *
   * <p>This method handles recursive cause structures that might
   * otherwise cause infinite loops. The cause chain is processed until
   * the end is reached, or until the next item in the chain is already
   * in the result set.</p>
   *
   * @param throwable  the throwable to inspect, may be null
   * @return the list of throwables, never null
   * @since 2.2
   */
  public static List<Throwable> getThrowableList(Throwable throwable) {
    final List<Throwable> list = new ArrayList<>();
    while (throwable != null && !list.contains(throwable)) {
      list.add(throwable);
      throwable = throwable.getCause();
    }
    return list;
  }

  /**
   * Gets a short message summarising the exception.
   * <p>
   * The message returned is of the form
   * {ClassNameWithoutPackage}: {ThrowableMessage}
   *
   * @param th  the throwable to get a message for, null returns empty string
   * @return the message, non-null
   * @since 2.2
   */
  public static String getMessage(final Throwable th) {
    if (th == null) {
      return StringUtils.EMPTY;
    }
    final String clsName = getSimpleName(th);
    final String msg = th.getMessage();
    return clsName + ": " + StringUtils.defaultString(msg);
  }

  private static String getSimpleName(Throwable th) {
    return th.getClass()
        .getSimpleName();
  }

  /**
   * <p>Gets the stack trace from a Throwable as a String.</p>
   *
   * <p>The result of this method vary by JDK version as this method
   * uses {@link Throwable#printStackTrace(java.io.PrintWriter)}.
   * On JDK1.3 and earlier, the cause exception will not be shown
   * unless the specified throwable alters printStackTrace.</p>
   *
   * @param throwable  the <code>Throwable</code> to be examined
   * @return the stack trace as generated by the exception's
   *  <code>printStackTrace(PrintWriter)</code> method
   */
  public static String getStackTrace(final Throwable throwable) {
    final StringWriter sw = new StringWriter();
    final PrintWriter pw = new PrintWriter(sw, true);
    throwable.printStackTrace(pw);
    return sw.getBuffer().toString();
  }
}
