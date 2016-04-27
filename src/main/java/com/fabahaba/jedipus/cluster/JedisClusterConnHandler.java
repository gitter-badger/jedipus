package com.fabahaba.jedipus.cluster;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

class JedisClusterConnHandler implements Closeable {

  private final JedisClusterSlotCache cache;

  JedisClusterConnHandler(final ReadMode defaultReadMode,
      final Collection<HostAndPort> discoveryHostPorts,
      final Function<HostAndPort, JedisPool> masterPoolFactory,
      final Function<HostAndPort, JedisPool> slavePoolFactory,
      final Function<JedisPool[], LoadBalancedPools> lbFactory) {

    this.cache = JedisClusterSlotCache.create(defaultReadMode, discoveryHostPorts,
        masterPoolFactory, slavePoolFactory, lbFactory);
  }

  ReadMode getDefaultReadMode() {

    return cache.getDefaultReadMode();
  }

  Jedis getConnection(final ReadMode readMode) {

    List<JedisPool> shuffledPools = cache.getShuffledPools(readMode);
    if (shuffledPools.isEmpty()) {

      renewSlotCache(readMode);
      shuffledPools = cache.getShuffledPools(readMode);
    }

    for (final JedisPool pool : shuffledPools) {

      Jedis jedis = null;
      try {
        jedis = pool.getResource();

        if (jedis == null) {
          continue;
        }

        if (jedis.ping().equalsIgnoreCase("pong")) {
          return jedis;
        }

        jedis.close();
      } catch (final JedisException ex) {
        if (jedis != null) {
          jedis.close();
        }
      }
    }

    throw new JedisConnectionException("no reachable node in cluster");
  }

  Jedis getConnectionFromSlot(final ReadMode readMode, final int slot) {

    final JedisPool connectionPool = cache.getSlotPool(readMode, slot);

    return connectionPool == null ? getConnection(readMode) : connectionPool.getResource();
  }

  Jedis getAskJedis(final HostAndPort hostPort) {

    return cache.getAskJedis(hostPort);
  }

  List<JedisPool> getMasterPools() {

    return cache.getMasterPools();
  }

  List<JedisPool> getSlavePools() {

    return cache.getSlavePools();
  }

  List<JedisPool> getAllPools() {

    return cache.getAllPools();
  }

  void renewSlotCache(final ReadMode readMode) {

    for (final JedisPool jp : cache.getShuffledPools(readMode)) {

      try (final Jedis jedis = jp.getResource()) {

        cache.discoverClusterSlots(jedis);
        return;
      } catch (final JedisConnectionException e) {
        // try next nodes
      }
    }

    for (final HostAndPort discoveryHostPort : cache.getDiscoveryHostPorts()) {

      try (
          final Jedis jedis = new Jedis(discoveryHostPort.getHost(), discoveryHostPort.getPort())) {

        cache.discoverClusterSlots(jedis);
        return;
      } catch (final JedisConnectionException e) {
        // try next nodes
      }
    }
  }

  void renewSlotCache(final ReadMode readMode, final Jedis jedis) {

    try {

      cache.discoverClusterSlots(jedis);
    } catch (final JedisConnectionException e) {

      renewSlotCache(readMode);
    }
  }

  @Override
  public void close() throws IOException {

    cache.close();
  }
}
