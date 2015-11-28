package com.fabahaba.jedipus;

import com.fabahaba.fava.logging.Loggable;
import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.pool2.ObjectPool;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

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
  private final PoolFactory poolFactory;

  private final StampedLock sentinelPoolLock;
  private ObjectPool<Jedis> sentinelPool;
  private int numConsecutiveConnectionFailures;

  public JedisSentinelPoolExecutor(final String masterName, final int db,
      final Collection<String> sentinelHostPorts, final String password) {

    this(masterName, db, sentinelHostPorts, password, null, PoolFactory.DEFAULT_FACTORY);
  }

  public JedisSentinelPoolExecutor(final String masterName, final int db,
      final Collection<String> sentinelHostPorts, final String password,
      final ExtendedJedisPoolConfig poolConfig, final PoolFactory poolFactory) {

    this.poolFactory = poolFactory;
    this.masterName = masterName;
    this.db = db;
    this.sentinelHostPorts = ImmutableSet.copyOf(sentinelHostPorts);
    this.password = password;
    this.poolConfig =
        poolConfig == null ? ExtendedJedisPoolConfig.getDefaultConfig() : poolConfig.copy();
    this.sentinelPoolLock = new StampedLock();
    this.numConsecutiveConnectionFailures = 0;
  }

  @Override
  public void acceptJedis(final Consumer<Jedis> jedisConsumer) {

    Jedis jedis = null;
    long readStamp = ensurePoolExists();
    try {
      jedis = sentinelPool.borrowObject();
      jedisConsumer.accept(jedis);
      numConsecutiveConnectionFailures = 0;
    } catch (final JedisConnectionException jce) {
      readStamp = handleConnectionException(readStamp, jedis);
      jedis = null;
      throw jce;
    } catch (final JedisDataException jde) {
      if (jde.getMessage().contains("READONLY")) {
        readStamp = handleConnectionException(readStamp, jedis);
        jedis = null;
      }
      throw jde;
    } catch (final Throwable t) {
      throw Throwables.propagate(t);
    } finally {
      readStamp = returnJedis(readStamp, jedis);
      if (readStamp > 0) {
        sentinelPoolLock.unlockRead(readStamp);
      }
    }
  }

  @Override
  public <R> R applyJedis(final Function<Jedis, R> jedisFunc) {

    Jedis jedis = null;
    long readStamp = ensurePoolExists();
    try {
      jedis = sentinelPool.borrowObject();
      final R result = jedisFunc.apply(jedis);
      numConsecutiveConnectionFailures = 0;
      return result;
    } catch (final JedisConnectionException jce) {
      readStamp = handleConnectionException(readStamp, jedis);
      jedis = null;
      throw jce;
    } catch (final JedisDataException jde) {
      if (jde.getMessage().contains("READONLY")) {
        readStamp = handleConnectionException(readStamp, jedis);
        jedis = null;
      }
      throw jde;
    } catch (final Throwable t) {
      throw Throwables.propagate(t);
    } finally {
      readStamp = returnJedis(readStamp, jedis);
      if (readStamp > 0) {
        sentinelPoolLock.unlockRead(readStamp);
      }
    }
  }

  private long returnJedis(final long readStamp, final Jedis jedis) {

    try {
      sentinelPool.returnObject(jedis);
      return readStamp;
    } catch (final JedisConnectionException je) {

      return handleConnectionException(readStamp, jedis);
    } catch (final JedisDataException jde) {

      if (jde.getMessage().contains("READONLY"))
        return handleConnectionException(readStamp, jedis);

      catching(jde);
      error("Failed to return jedis object.");
    } catch (final Throwable t) {
      catching(t);
      error("Failed to return jedis object.");
    }

    return readStamp;
  }

  private long handleConnectionException(final long readStamp, final Jedis brokenJedis) {

    try {
      sentinelPool.invalidateObject(brokenJedis);
    } catch (final Throwable t) {
      catching(t);
      error("Failed to invalidate jedis object.");
    }

    return reInitPool(readStamp);
  }

  private long ensurePoolExists() {

    for (;;) {
      final long readStamp = sentinelPoolLock.readLock();

      if (sentinelPool != null)
        return readStamp;

      sentinelPoolLock.unlockRead(readStamp);

      final long writeStamp = sentinelPoolLock.writeLock();
      try {
        if (sentinelPool == null) {
          sentinelPool =
              poolFactory.newPool(masterName, db, sentinelHostPorts, password, poolConfig);
          numConsecutiveConnectionFailures = 0;
        }
      } finally {
        sentinelPoolLock.unlockWrite(writeStamp);
      }
    }
  }

  private long reInitPool(final long readStamp) {

    final ObjectPool<Jedis> previouslyKnownPool = sentinelPool;

    sentinelPoolLock.unlockRead(readStamp);

    final long writeStamp = sentinelPoolLock.writeLock();
    try {
      if (sentinelPool != null
          && ++numConsecutiveConnectionFailures < poolConfig.getMaxConsecutiveFailures())
        return 0;

      if (previouslyKnownPool != null) {
        try {
          previouslyKnownPool.close();
        } catch (final Throwable t) {
          catching(t);
          error("Failed to close pool {}.", previouslyKnownPool);
        }
      }

      try {
        sentinelPool = null;
        sentinelPool = poolFactory.newPool(masterName, db, sentinelHostPorts, password, poolConfig);
        numConsecutiveConnectionFailures = 0;
      } catch (final Throwable t) {
        catching(t);
        error("Failed to create new pool.");
      }
    } finally {
      sentinelPoolLock.unlockWrite(writeStamp);
    }

    return 0;
  }

  public void shutDown() {

    final long writeStamp = sentinelPoolLock.writeLock();
    try {
      if (sentinelPool != null) {
        sentinelPool.close();
      }
    } finally {
      sentinelPoolLock.unlockWrite(writeStamp);
    }
  }

  @Override
  public String toString() {

    return MoreObjects.toStringHelper(JedisSentinelPoolExecutor.class).add("masterName", masterName)
        .add("db", db).add("sentinelHostPorts", sentinelHostPorts).add("poolConfig", poolConfig)
        .add("sentinelPool", sentinelPool).toString();
  }
}
