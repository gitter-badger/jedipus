package com.fabahaba.jedipus.cluster;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

class JedisClusterConnHandler implements Closeable {

  private final JedisClusterSlotCache cache;

  JedisClusterConnHandler(final Collection<HostAndPort> discoveryNodes,
      final Function<HostAndPort, JedisPool> jedisPoolFactory) {

    this.cache = JedisClusterSlotCache.create(discoveryNodes, jedisPoolFactory);
  }

  Jedis getConnection() {

    List<JedisPool> shuffledPools = cache.getShuffledPools();
    if (shuffledPools.isEmpty()) {

      renewSlotCache();
      shuffledPools = cache.getShuffledPools();
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

  Jedis getConnectionFromSlot(final int slot) {

    final JedisPool connectionPool = cache.getSlotPool(slot);

    return connectionPool == null ? getConnection() : connectionPool.getResource();
  }

  Jedis getConnectionFromNode(final HostAndPort node) {

    return cache.setNodeIfNotExist(node).getResource();
  }

  List<JedisPool> getPools() {

    return cache.getPools();
  }

  void renewSlotCache() {

    for (final JedisPool jp : cache.getShuffledPools()) {

      try (final Jedis jedis = jp.getResource()) {

        cache.discoverClusterSlots(jedis);
        return;
      } catch (final JedisConnectionException e) {
        // try next nodes
      }
    }

    for (final HostAndPort discoveryNode : cache.getDiscoveryNodes()) {

      try (final Jedis jedis = new Jedis(discoveryNode.getHost(), discoveryNode.getPort())) {

        cache.discoverClusterSlots(jedis);
        return;
      } catch (final JedisConnectionException e) {
        // try next nodes
      }
    }
  }

  void renewSlotCache(final Jedis jedis) {

    try {

      cache.discoverClusterSlots(jedis);
    } catch (final JedisConnectionException e) {

      renewSlotCache();
    }
  }

  @Override
  public void close() throws IOException {

    cache.close();
  }
}
