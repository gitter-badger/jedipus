package com.fabahaba.jedipus;

import com.fabahaba.fava.logging.Loggable;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Pool;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;
import java.util.function.Function;

public class JedisSentinelPoolExecutor implements JedisExecutor, Loggable {

  private final String masterName;
  private final int db;
  private final Set<String> sentinelHostPorts;
  private final String password;
  private final ExtendedJedisPoolConfig poolConfig;

  private final StampedLock sentinelPoolLock;
  private Pool<Jedis> sentinelPool;
  private int numConsecutiveFailures;

  public JedisSentinelPoolExecutor(final String masterName, final int db,
      final Collection<String> sentinelHostPorts, final String password) {
    this(masterName, db, sentinelHostPorts, password, null);
  }

  public JedisSentinelPoolExecutor(final String masterName, final int db,
      final Collection<String> sentinelHostPorts, final String password,
      final ExtendedJedisPoolConfig poolConfig) {
    this.masterName = masterName;
    this.db = db;
    this.sentinelHostPorts = ImmutableSet.copyOf(sentinelHostPorts);
    this.password = password;
    this.poolConfig =
        poolConfig == null ? ExtendedJedisPoolConfig.getDefaultConfig() : poolConfig.copy();
    this.sentinelPoolLock = new StampedLock();
    this.numConsecutiveFailures = 0;
  }

  @Override
  public void acceptJedis(final Consumer<Jedis> jedisConsumer) {
    Jedis jedis = null;
    long readStamp = ensurePoolExists();
    try {
      jedis = sentinelPool.getResource();
      jedisConsumer.accept(jedis);
    } catch (final JedisException je) {
      readStamp = handleConnectionException(readStamp, jedis);
      jedis = null;
      throw je;
    } finally {
      readStamp = returnJedis(readStamp, jedis);
      if (readStamp > 0) {
        sentinelPoolLock.unlockRead(readStamp);
      }
    }
  }

  @Override
  public <T> T applyJedis(final Function<Jedis, T> jedisFunc) {
    Jedis jedis = null;
    long readStamp = ensurePoolExists();
    try {
      jedis = sentinelPool.getResource();
      return jedisFunc.apply(jedis);
    } catch (final JedisException je) {
      readStamp = handleConnectionException(readStamp, jedis);
      jedis = null;
      throw je;
    } finally {
      readStamp = returnJedis(readStamp, jedis);
      if (readStamp > 0) {
        sentinelPoolLock.unlockRead(readStamp);
      }
    }
  }

  private long returnJedis(final long readStamp, final Jedis jedis) {
    try {
      sentinelPool.returnResourceObject(jedis);
      numConsecutiveFailures = 0;
      return readStamp;
    } catch (final JedisException je) {
      return handleConnectionException(readStamp, jedis);
    }
  }

  private long handleConnectionException(final long readStamp, final Jedis brokenJedis) {
    try {
      sentinelPool.returnBrokenResource(brokenJedis);
    } catch (final JedisException je) {
      catching(je);
    }

    return tryToReInitPool(readStamp);
  }

  private Pool<Jedis> constructPool() {
    return new JedisSentinelPool(masterName, sentinelHostPorts, poolConfig,
        poolConfig.getConnectionTimeoutMillis(), password, db);
  }

  private long ensurePoolExists() {
    final long readStamp = sentinelPoolLock.readLock();
    if (sentinelPool != null)
      return readStamp;

    sentinelPoolLock.unlockRead(readStamp);
    final long writeStamp = sentinelPoolLock.writeLock();
    try {
      if (sentinelPool == null) {
        sentinelPool = constructPool();
      }
    } finally {
      sentinelPoolLock.unlockWrite(writeStamp);
    }
    return sentinelPoolLock.readLock();
  }

  private long tryToReInitPool(final long readStamp) {
    final Pool<Jedis> previouslyKnownPool = sentinelPool;

    sentinelPoolLock.unlockRead(readStamp);
    final long writeStamp = sentinelPoolLock.writeLock();
    try {
      if (sentinelPool == null || sentinelPool.equals(previouslyKnownPool)
          && ++numConsecutiveFailures > poolConfig.getMaxConsecutiveFailures()) {
        try {
          sentinelPool = constructPool();
          numConsecutiveFailures = 0;
        } catch (final Throwable t) {
          error(t);
        }
      }
    } finally {
      sentinelPoolLock.unlockWrite(writeStamp);
    }
    return 0;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(JedisSentinelPoolExecutor.class)
        .add("masterName", masterName).add("db", db).add("sentinelHostPorts", sentinelHostPorts)
        .add("poolConfig", poolConfig).add("sentinelPool", sentinelPool).toString();
  }
}
