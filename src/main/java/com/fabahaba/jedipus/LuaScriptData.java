package com.fabahaba.jedipus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import javax.xml.bind.DatatypeConverter;

import com.fabahaba.jedipus.cluster.JedisClusterExecutor;

import redis.clients.jedis.Jedis;

public class LuaScriptData implements LuaScript {

  private static final ThreadLocal<MessageDigest> SHA1 = ThreadLocal.withInitial(() -> {
    try {
      return MessageDigest.getInstance("SHA-1");
    } catch (final NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  });

  private final String luaScript;
  private final String sha1Hex;
  private final byte[] sha1HexBytes;

  public LuaScriptData(final String luaScript) {

    this.luaScript = luaScript;
    this.sha1Hex = DatatypeConverter
        .printHexBinary(SHA1.get().digest(luaScript.getBytes(StandardCharsets.UTF_8)))
        .toLowerCase(Locale.ENGLISH);
    this.sha1HexBytes = sha1Hex.getBytes(StandardCharsets.UTF_8);
  }

  public static LuaScriptData fromResourcePath(final String resourcePath) {

    try (final BufferedReader reader = new BufferedReader(new InputStreamReader(
        LuaScriptData.class.getResourceAsStream(resourcePath), StandardCharsets.UTF_8))) {

      final String luaScript = reader.lines().filter(l -> !l.isEmpty() && !l.contains("--"))
          .collect(Collectors.joining(" ")).replaceAll("\\s+", " ");

      return new LuaScriptData(luaScript);
    } catch (final IOException e) {

      throw new UncheckedIOException(e);
    }
  }

  @Override
  public Object eval(final Jedis jedis, final int numRetries, final int keyCount,
      final byte[]... params) {

    return jedis.evalsha(sha1HexBytes, keyCount, params);
  }

  @Override
  public Object eval(final Jedis jedis, final int numRetries, final List<byte[]> keys,
      final List<byte[]> args) {

    return jedis.evalsha(sha1HexBytes, keys, args);
  }

  @Override
  public Object eval(final Jedis jedis, final int keyCount, final byte[]... params) {

    return jedis.evalsha(sha1HexBytes, keyCount, params);
  }

  @Override
  public Object eval(final Jedis jedis, final List<byte[]> keys, final List<byte[]> args) {

    return jedis.evalsha(sha1HexBytes, keys, args);
  }

  @Override
  public Object eval(final JedisClusterExecutor jedisExecutor, final int numRetries,
      final int keyCount, final byte[]... params) {

    return jedisExecutor.applyJedis(params[0],
        jedis -> jedis.evalsha(sha1HexBytes, keyCount, params), numRetries);
  }

  @Override
  public Object eval(final JedisClusterExecutor jedisExecutor, final int numRetries,
      final List<byte[]> keys, final List<byte[]> args) {

    return jedisExecutor.applyJedis(keys.get(0), jedis -> jedis.evalsha(sha1HexBytes, keys, args),
        numRetries);
  }

  @Override
  public Object eval(final int slot, final JedisClusterExecutor jedisExecutor, final int numRetries,
      final int keyCount, final byte[]... params) {

    return jedisExecutor.applyJedis(slot, jedis -> jedis.evalsha(sha1HexBytes, keyCount, params),
        numRetries);
  }

  @Override
  public Object eval(final int slot, final JedisClusterExecutor jedisExecutor, final int numRetries,
      final List<byte[]> keys, final List<byte[]> args) {

    return jedisExecutor.applyJedis(slot, jedis -> jedis.evalsha(sha1HexBytes, keys, args),
        numRetries);
  }

  @Override
  public String getLuaScript() {

    return luaScript;
  }

  @Override
  public byte[] getSha1HexBytes() {

    return sha1HexBytes;
  }

  @Override
  public String getSha1Hex() {

    return sha1Hex;
  }

  @Override
  public boolean equals(final Object other) {

    if (this == other)
      return true;
    if (other == null)
      return false;
    if (!getClass().equals(other.getClass()))
      return false;
    final LuaScriptData castOther = LuaScriptData.class.cast(other);
    return Arrays.equals(sha1HexBytes, castOther.sha1HexBytes);
  }

  @Override
  public int hashCode() {

    return sha1HexBytes[0] << 24 | (sha1HexBytes[1] & 0xFF) << 16 | (sha1HexBytes[2] & 0xFF) << 8
        | (sha1HexBytes[3] & 0xFF);
  }

  @Override
  public String toString() {

    return String.format("LuaScriptData %s:%n%n%s]", sha1Hex, luaScript);
  }
}
