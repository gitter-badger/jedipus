package com.fabahaba.jedipus;

import com.fabahaba.jedipus.JedisExecutor;
import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;
import com.google.common.hash.Hashing;
import com.google.common.io.Resources;

import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class LuaScriptData implements LuaScript {

  private final String luaScript;
  private final String sha1;
  private final ByteBuffer sha1ByteBuffer;
  private final byte[] sha1Bytes;

  public LuaScriptData(final String resourcePath) {

    try {
      this.luaScript = Resources
          .readLines(Resources.getResource(LuaScriptData.class, resourcePath),
              StandardCharsets.UTF_8)
          .stream().filter(l -> !l.isEmpty() && !l.contains("--")).collect(Collectors.joining(" "))
          .replaceAll("\\s+", " ");

      this.sha1 = Hashing.sha1().hashString(luaScript, StandardCharsets.UTF_8).toString();
      this.sha1Bytes = sha1.getBytes(StandardCharsets.UTF_8);
      this.sha1ByteBuffer = ByteBuffer.wrap(sha1Bytes);
    } catch (final IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Object eval(final JedisExecutor jedisExecutor, final int numRetries, final int keyCount,
      final byte[]... params) {

    return jedisExecutor.applyJedis(jedis -> jedis.evalsha(sha1Bytes, keyCount, params),
        numRetries);
  }

  @Override
  public Object eval(final JedisExecutor jedisExecutor, final int numRetries,
      final List<byte[]> keys, final List<byte[]> args) {

    return jedisExecutor.applyJedis(jedis -> jedis.evalsha(sha1Bytes, keys, args), numRetries);
  }

  @Override
  public Object eval(final Jedis jedis, final int keyCount, final byte[]... params) {

    return jedis.evalsha(sha1Bytes, keyCount, params);
  }

  @Override
  public Object eval(final Jedis jedis, final List<byte[]> keys, final List<byte[]> args) {

    return jedis.evalsha(sha1Bytes, keys, args);
  }

  @Override
  public String getLuaScript() {

    return luaScript;
  }

  @Override
  public String getSha1() {

    return this.sha1;
  }

  @Override
  public ByteBuffer getSha1Bytes() {

    return sha1ByteBuffer;
  }

  @Override
  public String toString() {

    return MoreObjects.toStringHelper(this).add("sha1", sha1).add("luaScript", luaScript)
        .toString();
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
    return Arrays.equals(sha1Bytes, castOther.sha1Bytes);
  }

  @Override
  public int hashCode() {

    return Arrays.hashCode(sha1Bytes);
  }
}
