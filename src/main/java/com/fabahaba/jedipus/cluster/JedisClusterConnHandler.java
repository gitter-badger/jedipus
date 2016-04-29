package com.fabahaba.jedipus.cluster;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

class JedisClusterConnHandler implements AutoCloseable {

  private final JedisClusterSlotCache slotPoolCache;

  JedisClusterConnHandler(final ReadMode defaultReadMode, final boolean optimisticReads,
      final Collection<HostAndPort> discoveryHostPorts,
      final Function<HostAndPort, JedisPool> masterPoolFactory,
      final Function<HostAndPort, JedisPool> slavePoolFactory,
      final Function<HostAndPort, Jedis> jedisAskFactory,
      final Function<JedisPool[], LoadBalancedPools> lbFactory, final boolean initReadOnly) {

    this.slotPoolCache =
        JedisClusterSlotCache.create(defaultReadMode, optimisticReads, discoveryHostPorts,
            masterPoolFactory, slavePoolFactory, jedisAskFactory, lbFactory, initReadOnly);
  }

  ReadMode getDefaultReadMode() {

    return slotPoolCache.getDefaultReadMode();
  }

  boolean isInitReadOnly() {

    return slotPoolCache.isInitReadOnly();
  }

  Jedis getConnection(final ReadMode readMode) {

    return getConnection(readMode, -1);
  }

  Jedis getConnection(final ReadMode readMode, final int slot) {

    List<JedisPool> shuffledPools = slotPoolCache.getShuffledPools(readMode);
    if (shuffledPools.isEmpty()) {

      renewSlotCache(readMode);
      if (slot >= 0) {
        final Jedis jedis = slotPoolCache.getSlotConnection(readMode, slot);
        if (jedis != null) {
          return jedis;
        }
      }

      shuffledPools = slotPoolCache.getShuffledPools(readMode);
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

    final Jedis jedis = slotPoolCache.getSlotConnection(readMode, slot);

    return jedis == null ? getConnection(readMode, slot) : jedis;
  }

  Jedis getAskJedis(final HostAndPort hostPort) {

    return slotPoolCache.getAskJedis(hostPort);
  }

  List<JedisPool> getMasterPools() {

    return slotPoolCache.getMasterPools();
  }

  List<JedisPool> getSlavePools() {

    return slotPoolCache.getSlavePools();
  }

  List<JedisPool> getAllPools() {

    return slotPoolCache.getAllPools();
  }

  JedisPool getMasterPoolIfPresent(final HostAndPort hostPort) {

    return slotPoolCache.getMasterPoolIfPresent(hostPort);
  }

  JedisPool getSlavePoolIfPresent(final HostAndPort hostPort) {

    return slotPoolCache.getSlavePoolIfPresent(hostPort);
  }

  JedisPool getPoolIfPresent(final HostAndPort hostPort) {

    return slotPoolCache.getPoolIfPresent(hostPort);
  }

  void renewSlotCache(final ReadMode readMode) {

    for (final JedisPool jp : slotPoolCache.getShuffledPools(readMode)) {

      try (final Jedis jedis = jp.getResource()) {

        slotPoolCache.discoverClusterSlots(jedis);
        return;
      } catch (final JedisConnectionException e) {
        // try next nodes
      }
    }

    for (final HostAndPort discoveryHostPort : slotPoolCache.getDiscoveryHostPorts()) {

      try (
          final Jedis jedis = new Jedis(discoveryHostPort.getHost(), discoveryHostPort.getPort())) {

        slotPoolCache.discoverClusterSlots(jedis);
        return;
      } catch (final JedisConnectionException e) {
        // try next nodes
      }
    }
  }

  void renewSlotCache(final ReadMode readMode, final Jedis jedis) {

    try {

      slotPoolCache.discoverClusterSlots(jedis);
    } catch (final JedisConnectionException e) {

      renewSlotCache(readMode);
    }
  }

  @Override
  public void close() {

    slotPoolCache.close();
  }
}
