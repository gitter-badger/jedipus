package com.fabahaba.jedipus.cluster;

public final class RCUtils {

  private RCUtils() {}

  public static String createHashTag(final String shardKey) {

    return "{" + shardKey + "}";
  }

  public static final String NAMESPACE_DELIM = ":";

  public static String createNameSpacedHashTag(final String shardKey) {

    return createNameSpacedHashTag(shardKey, NAMESPACE_DELIM);
  }

  public static String createNameSpacedHashTag(final String shardKey, final String namespaceDelim) {

    return createHashTag(shardKey) + namespaceDelim;
  }

  public static String prefixHashTag(final String shardKey, final String postFix) {

    return createHashTag(shardKey) + postFix;
  }

  public static String prefixNameSpacedHashTag(final String shardKey, final String postFix) {

    return prefixNameSpacedHashTag(shardKey, NAMESPACE_DELIM, postFix);
  }

  public static String prefixNameSpacedHashTag(final String shardKey, final String namespaceDelim,
      final String postFix) {

    return createNameSpacedHashTag(shardKey, namespaceDelim) + postFix;
  }
}
