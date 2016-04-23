package com.fabahaba.jedipus;

import java.util.List;
import java.util.stream.Stream;

import com.fabahaba.jedipus.cluster.JedisClusterExecutor;

import redis.clients.jedis.Jedis;

public interface LuaScript {

  public String getLuaScript();

  public String getSha1Hex();

  public byte[] getSha1HexBytes();

  public Object eval(final Jedis jedis, final int numRetries, final int keyCount,
      final byte[]... params);

  public Object eval(final Jedis jedis, final int numRetries, final List<byte[]> keys,
      final List<byte[]> args);

  public Object eval(final Jedis jedis, final int keyCount, final byte[]... params);

  public Object eval(final Jedis jedis, final List<byte[]> keys, final List<byte[]> args);

  public static void loadIfNotExists(final Jedis jedis, final byte[][] scriptSha1Bytes,
      final LuaScript[] luaScripts) {

    final List<Long> existResults = jedis.scriptExists(scriptSha1Bytes);

    int index = 0;
    for (final long exists : existResults) {
      if (exists == 0) {
        jedis.scriptLoad(luaScripts[index].getLuaScript());
      }
      index++;
    }
  }

  public Object eval(final JedisClusterExecutor jedisExecutor, final int numRetries,
      final int keyCount, final byte[]... params);

  public Object eval(final JedisClusterExecutor jedisExecutor, final int numRetries,
      final List<byte[]> keys, final List<byte[]> args);

  public Object eval(final JedisClusterExecutor jedisExecutor, final int numRetries,
      final int keyCount, final int slotKey, final byte[]... params);

  public Object eval(final JedisClusterExecutor jedisExecutor, final int numRetries,
      final int slotKey, final List<byte[]> keys, final List<byte[]> args);

  public static void loadMissingScripts(final JedisClusterExecutor jedisExecutor,
      final LuaScript... luaScripts) {

    final byte[][] scriptSha1Bytes =
        Stream.of(luaScripts).map(LuaScript::getSha1HexBytes).toArray(byte[][]::new);

    jedisExecutor.acceptAllMasters(jedis -> loadIfNotExists(jedis, scriptSha1Bytes, luaScripts));
  }
}
