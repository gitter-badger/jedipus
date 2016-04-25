package com.fabahaba.jedipus;

import java.nio.charset.StandardCharsets;

public final class RESP {

  private RESP() {}

  public static byte[] toBytes(final int num) {

    return toBytes(String.valueOf(num));
  }

  public static byte[] toBytes(final long num) {

    return toBytes(String.valueOf(num));
  }

  public static byte[] toBytes(final String string) {

    return string.getBytes(StandardCharsets.UTF_8);
  }

  public static String toString(final Object bytes) {

    return toString((byte[]) bytes);
  }

  public static String toString(final byte[] bytes) {

    return new String(bytes, StandardCharsets.UTF_8);
  }

  public static Integer toInt(final Object bytes) {

    return Integer.parseInt(toString(bytes));
  }

  public static Integer toInt(final byte[] bytes) {

    return Integer.parseInt(toString(bytes));
  }

  public static Long toLong(final Object bytes) {

    return Long.parseLong(toString(bytes));
  }

  public static Long toLong(final byte[] bytes) {

    return Long.parseLong(toString(bytes));
  }
}
