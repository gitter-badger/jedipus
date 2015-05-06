package com.fabahaba.jedipus;

import com.fabahaba.fava.logging.Loggable;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Sets;

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
    this.sentinelHostPorts = Sets.newHashSet(sentinelHostPorts);
    this.password = password;
    this.poolConfig =
        poolConfig == null ? ExtendedJedisPoolConfig.getDefaultConfig() : poolConfig.copy();
    this.sentinelPoolLock = new StampedLock();
    this.numConsecutiveFailures = 0;
  }

  @Override
  public void acceptJedis(final Consumer<Jedis> jedisConsumer) {
    long readStamp = sentinelPoolLock.readLock();
    try {
      Jedis jedis = null;
      readStamp = ensurePoolExists(readStamp);
      try {
        jedis = sentinelPool.getResource();
        jedisConsumer.accept(jedis);
      } catch (final JedisException je) {
        readStamp = handleConnectionException(readStamp, jedis);
        jedis = null;
        throw je;
      } finally {
        returnJedis(jedis);
      }
    } finally {
      sentinelPoolLock.unlock(readStamp);
    }
  }

  @Override
  public <T> T applyJedis(final Function<Jedis, T> jedisFunc) {
    long readStamp = sentinelPoolLock.readLock();
    try {
      Jedis jedis = null;
      readStamp = ensurePoolExists(readStamp);
      try {
        jedis = sentinelPool.getResource();
        return jedisFunc.apply(jedis);
      } catch (final JedisException je) {
        readStamp = handleConnectionException(readStamp, jedis);
        jedis = null;
        throw je;
      } finally {
        returnJedis(jedis);
      }
    } finally {
      sentinelPoolLock.unlock(readStamp);
    }
  }

  private void returnJedis(final Jedis jedis) {
    try {
      if (jedis != null) {
        sentinelPool.returnResource(jedis);
        numConsecutiveFailures = 0;
      }
    } catch (final JedisException je) {
      catching(je);
    }
  }

  private long handleConnectionException(final long lockStamp, final Jedis brokenJedis) {
    try {
      if (brokenJedis != null) {
        sentinelPool.returnBrokenResource(brokenJedis);
      }
    } catch (final JedisException je) {
      catching(je);
    }

    return tryToReInitPool(lockStamp);
  }

  private Pool<Jedis> constructPool() {
    return new JedisSentinelPool(masterName, sentinelHostPorts, poolConfig,
        poolConfig.getConnectionTimeoutMillis(), password, db);
  }

  private long ensurePoolExists(final long lockStamp) {
    if (sentinelPool != null)
      return lockStamp;

    long stamp = sentinelPoolLock.tryConvertToWriteLock(lockStamp);
    if (stamp == 0) {
      if (sentinelPoolLock.isReadLocked()) {
        sentinelPoolLock.unlockRead(lockStamp);
      }

      if (sentinelPool == null) {
        stamp = sentinelPoolLock.writeLock();
      } else
        return sentinelPoolLock.readLock();
    }

    try {
      if (sentinelPool == null) {
        sentinelPool = constructPool();
      }
    } finally {
      stamp = sentinelPoolLock.tryConvertToReadLock(stamp);
    }
    return stamp;
  }

  private long tryToReInitPool(final long lockStamp) {
    final Pool<Jedis> previouslyKnownPool = sentinelPool;

    long stamp = sentinelPoolLock.tryConvertToWriteLock(lockStamp);
    if (stamp == 0) {
      if (sentinelPoolLock.isReadLocked()) {
        sentinelPoolLock.unlockRead(lockStamp);
      }

      final Pool<Jedis> currentPool = sentinelPool;
      if (currentPool != null && currentPool.equals(previouslyKnownPool)) {
        stamp = sentinelPoolLock.writeLock();
      } else
        return sentinelPoolLock.readLock();
    }

    try {
      if (sentinelPool.equals(previouslyKnownPool)) {
        if (++numConsecutiveFailures > poolConfig.getMaxConsecutiveFailures()) {
          sentinelPool = null;
          numConsecutiveFailures = 0;
          stamp = ensurePoolExists(stamp);
        }
      }
    } finally {
      if (sentinelPoolLock.isWriteLocked()) {
        stamp = sentinelPoolLock.tryConvertToReadLock(stamp);
      }
    }
    return stamp;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(JedisSentinelPoolExecutor.class)
        .add("masterName", masterName).add("db", db).add("sentinelHostPorts", sentinelHostPorts)
        .add("poolConfig", poolConfig).add("sentinelPool", sentinelPool).toString();
  }
}
