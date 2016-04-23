package com.fabahaba.jedipus.cluster;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;

import redis.clients.jedis.BinaryJedisCluster;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

final class JedisClusterSlotCache implements Closeable {

  private final Set<HostAndPort> discoveryNodes;
  private final Map<HostAndPort, JedisPool> nodes;
  private final JedisPool[] slots;

  private final StampedLock lock;

  private final Function<HostAndPort, JedisPool> jedisPoolFactory;

  private static final int MASTER_NODE_INDEX = 2;

  private JedisClusterSlotCache(final Set<HostAndPort> discoveryNodes,
      final Map<HostAndPort, JedisPool> nodes, final JedisPool[] slots,
      final Function<HostAndPort, JedisPool> jedisPoolFactory) {

    this.discoveryNodes = discoveryNodes;
    this.nodes = nodes;
    this.slots = slots;
    this.lock = new StampedLock();

    this.jedisPoolFactory = jedisPoolFactory;
  }

  @SuppressWarnings("unchecked")
  public static JedisClusterSlotCache create(final Set<HostAndPort> discoveryNodes,
      final Function<HostAndPort, JedisPool> jedisPoolFactory) {

    final Map<HostAndPort, JedisPool> nodes = new HashMap<>();
    final JedisPool[] slotArray = new JedisPool[BinaryJedisCluster.HASHSLOTS];

    final Set<HostAndPort> allDiscoveryNodes = new HashSet<>(discoveryNodes);

    for (final HostAndPort discoveryNode : discoveryNodes) {

      try (final Jedis jedis = new Jedis(discoveryNode.getHost(), discoveryNode.getPort())) {

        final List<Object> slots = jedis.clusterSlots();

        for (final Object slotInfoObj : slots) {

          final List<Object> slotInfo = (List<Object>) slotInfoObj;

          for (int i = MASTER_NODE_INDEX, slotInfoSize = slotInfo.size(); i < slotInfoSize; i++) {

            final List<Object> hostInfos = (List<Object>) slotInfo.get(i);
            if (hostInfos.isEmpty()) {
              continue;
            }

            final HostAndPort targetNode = generateHostAndPort(hostInfos);
            allDiscoveryNodes.add(discoveryNode);

            if (i == MASTER_NODE_INDEX) {

              final JedisPool targetPool = jedisPoolFactory.apply(targetNode);
              nodes.put(targetNode, targetPool);

              Arrays.fill(slotArray, ((Long) slotInfo.get(0)).intValue(),
                  ((Long) slotInfo.get(1)).intValue(), targetPool);
            }
          }
        }

        return new JedisClusterSlotCache(allDiscoveryNodes, nodes, slotArray, jedisPoolFactory);
      } catch (final JedisConnectionException e) {
        // try next discoveryNode...
      }
    }

    return new JedisClusterSlotCache(allDiscoveryNodes, nodes, slotArray, jedisPoolFactory);
  }

  @SuppressWarnings("unchecked")
  void discoverClusterSlots(final Jedis jedis) {

    final long writeStamp = lock.writeLock();
    try {

      nodes.clear();
      Arrays.fill(slots, null);

      final List<Object> slots = jedis.clusterSlots();

      for (final Object slotInfoObj : slots) {

        final List<Object> slotInfo = (List<Object>) slotInfoObj;

        for (int i = MASTER_NODE_INDEX, slotInfoSize = slotInfo.size(); i < slotInfoSize; i++) {

          final List<Object> hostInfos = (List<Object>) slotInfo.get(i);
          if (hostInfos.isEmpty()) {
            continue;
          }

          final HostAndPort targetNode = generateHostAndPort(hostInfos);
          discoveryNodes.add(targetNode);

          if (i == MASTER_NODE_INDEX) {

            final JedisPool targetPool = jedisPoolFactory.apply(targetNode);
            nodes.put(targetNode, targetPool);

            Arrays.fill(this.slots, ((Long) slotInfo.get(0)).intValue(),
                ((Long) slotInfo.get(1)).intValue(), targetPool);
          }
        }
      }
    } finally {
      lock.unlockWrite(writeStamp);
    }
  }

  private static HostAndPort generateHostAndPort(final List<Object> hostInfos) {

    return new HostAndPort(new String((byte[]) hostInfos.get(0), StandardCharsets.UTF_8),
        ((Long) hostInfos.get(1)).intValue());
  }

  JedisPool setNodeIfNotExist(final HostAndPort node) {

    final long writeStamp = lock.writeLock();
    try {

      return nodes.computeIfAbsent(node, jedisPoolFactory);
    } finally {
      lock.unlockWrite(writeStamp);
    }
  }

  JedisPool getSlotPool(final int slot) {

    long readStamp = lock.tryOptimisticRead();

    JedisPool pool = slots[slot];

    if (!lock.validate(readStamp)) {

      readStamp = lock.readLock();
      try {
        pool = slots[slot];
      } finally {
        lock.unlockRead(readStamp);
      }
    }
    return pool;
  }

  List<JedisPool> getShuffledPools() {

    final List<JedisPool> pools = getPools();
    Collections.shuffle(pools);
    return pools;
  }

  List<JedisPool> getPools() {

    final long readStamp = lock.readLock();
    try {
      return new ArrayList<>(nodes.values());
    } finally {
      lock.unlockRead(readStamp);
    }
  }

  public Set<HostAndPort> getDiscoveryNodes() {
    return discoveryNodes;
  }

  @Override
  public void close() throws IOException {

    final long writeStamp = lock.writeLock();
    try {

      nodes.forEach((key, pool) -> {
        try {
          if (pool != null) {
            pool.destroy();
          }
        } catch (final Exception e) {
          // closing anyways...
        }
      });

      nodes.clear();
    } finally {
      lock.unlockWrite(writeStamp);
    }
  }
}
