package com.fabahaba.jedipus;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Stream;

import redis.clients.jedis.Jedis;

public interface LuaScript {

  public String getLuaScript();

  public String getSha1();

  public ByteBuffer getSha1Bytes();

  default void loadIfMissing(final JedisExecutor jedisExecutor) {

    LuaScript.loadMissingScripts(jedisExecutor, this);
  }

  public Object eval(final JedisExecutor jedisExecutor, final int numRetries, final int keyCount,
      final byte[]... params);

  public Object eval(final Jedis jedis, final int numRetries, final int keyCount,
      final byte[]... params);

  public Object eval(final JedisExecutor jedisExecutor, final int numRetries,
      final List<byte[]> keys, final List<byte[]> args);

  public Object eval(final Jedis jedis, final int numRetries, final List<byte[]> keys,
      final List<byte[]> args);

  public Object eval(final Jedis jedis, final int keyCount, final byte[]... params);

  public Object eval(final Jedis jedis, final List<byte[]> keys, final List<byte[]> args);

  public static void loadMissingScripts(final JedisExecutor jedisExecutor,
      final LuaScript... luaScripts) {

    final byte[][] scriptSha1Bytes = Stream.of(luaScripts).map(LuaScript::getSha1Bytes)
        .map(ByteBuffer::array).toArray(byte[][]::new);

    jedisExecutor.acceptJedis(jedis -> {
      final List<Long> existResults = jedis.scriptExists(scriptSha1Bytes);

      for (int i = 0; i < existResults.size(); i++) {
        if (existResults.get(i) == 0) {
          jedis.scriptLoad(luaScripts[i].getLuaScript());
        }
      }
    });
  }
}
