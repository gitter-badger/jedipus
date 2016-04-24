package com.fabahaba.jedipus.cluster;

public final class RCUtils {

  private RCUtils() {}

  public static String createHashTag(final String shardKey) {

    return "{" + shardKey + "}";
  }

  public static String createNameSpacedHashTag(final String shardKey) {

    return "{" + shardKey + "}:";
  }
}
