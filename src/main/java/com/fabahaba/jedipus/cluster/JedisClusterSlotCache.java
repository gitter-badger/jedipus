package com.fabahaba.jedipus.cluster;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;

import com.fabahaba.jedipus.RESP;
import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;

import redis.clients.jedis.BinaryJedisCluster;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

final class JedisClusterSlotCache implements Closeable {

  private final ReadMode defaultReadMode;

  private final Set<HostAndPort> discoveryHostPorts;

  private final Map<HostAndPort, JedisPool> masterPools;
  private final JedisPool[] masterSlots;

  private final Map<HostAndPort, JedisPool> slavePools;
  private final LoadBalancedPools[] slaveSlots;

  private final StampedLock lock;
  private volatile int slotDiscoveryCnt = 0;

  private final Function<HostAndPort, JedisPool> masterPoolFactory;
  private final Function<HostAndPort, JedisPool> slavePoolFactory;

  private static final int MASTER_NODE_INDEX = 2;

  private JedisClusterSlotCache(final ReadMode readMode, final Set<HostAndPort> discoveryNodes,
      final Map<HostAndPort, JedisPool> masterPools, final JedisPool[] masterSlots,
      final Map<HostAndPort, JedisPool> slavePools, final LoadBalancedPools[] slaveSlots,
      final Function<HostAndPort, JedisPool> masterPoolFactory,
      final Function<HostAndPort, JedisPool> slavePoolFactory) {

    this.defaultReadMode = readMode;
    this.discoveryHostPorts = discoveryNodes;

    this.masterPools = masterPools;
    this.masterSlots = masterSlots;

    this.slavePools = slavePools;
    this.slaveSlots = slaveSlots;

    this.lock = new StampedLock();

    this.masterPoolFactory = masterPoolFactory;
    this.slavePoolFactory = slavePoolFactory;
  }

  ReadMode getDefaultReadMode() {

    return defaultReadMode;
  }

  static JedisClusterSlotCache create(final ReadMode readMode,
      final Collection<HostAndPort> discoveryHostPorts,
      final Function<HostAndPort, JedisPool> masterPoolFactory,
      final Function<HostAndPort, JedisPool> slavePoolFactory) {

    final Map<HostAndPort, JedisPool> masterPools =
        readMode == ReadMode.SLAVES ? Collections.emptyMap() : new HashMap<>();
    final JedisPool[] masterSlots = readMode == ReadMode.SLAVES ? new JedisPool[0]
        : new JedisPool[BinaryJedisCluster.HASHSLOTS];

    final Map<HostAndPort, JedisPool> slavePools =
        readMode == ReadMode.MASTER ? Collections.emptyMap() : new HashMap<>();
    final LoadBalancedPools[] slaveSlots = readMode == ReadMode.MASTER ? new LoadBalancedPools[0]
        : new LoadBalancedPools[BinaryJedisCluster.HASHSLOTS];

    return create(readMode, discoveryHostPorts, masterPoolFactory, slavePoolFactory, masterPools,
        masterSlots, slavePools, slaveSlots);
  }

  @SuppressWarnings("unchecked")
  private static JedisClusterSlotCache create(final ReadMode readMode,
      final Collection<HostAndPort> discoveryHostPorts,
      final Function<HostAndPort, JedisPool> masterPoolFactory,
      final Function<HostAndPort, JedisPool> slavePoolFactory,
      final Map<HostAndPort, JedisPool> masterPools, final JedisPool[] masterSlots,
      final Map<HostAndPort, JedisPool> slavePools, final LoadBalancedPools[] slaveSlots) {

    final Set<HostAndPort> allDiscoveryHostPorts = new HashSet<>(discoveryHostPorts);

    for (final HostAndPort discoveryHostPort : discoveryHostPorts) {

      try (
          final Jedis jedis = new Jedis(discoveryHostPort.getHost(), discoveryHostPort.getPort())) {

        final List<Object> slots = jedis.clusterSlots();

        for (final Object slotInfoObj : slots) {

          final List<Object> slotInfo = (List<Object>) slotInfoObj;

          final int slotBegin = ((Long) slotInfo.get(0)).intValue();
          final int slotEnd = ((Long) slotInfo.get(1)).intValue();


          switch (readMode) {
            case MIXED_SLAVES:
            case MIXED:
            case MASTER:
              final HostAndPort masterHostPort =
                  generateHostAndPort((List<Object>) slotInfo.get(MASTER_NODE_INDEX));
              allDiscoveryHostPorts.add(masterHostPort);

              final JedisPool masterPool = masterPoolFactory.apply(masterHostPort);
              masterPools.put(masterHostPort, masterPool);

              Arrays.fill(masterSlots, slotBegin, slotEnd, masterPool);
              break;
            default:
            case SLAVES:
              break;
          }

          final ArrayList<JedisPool> slotSlavePools =
              readMode == ReadMode.MASTER ? null : new ArrayList<>(2);

          for (int i = MASTER_NODE_INDEX + 1, slotInfoSize =
              slotInfo.size(); i < slotInfoSize; i++) {

            final HostAndPort slaveHostPort = generateHostAndPort((List<Object>) slotInfo.get(i));
            allDiscoveryHostPorts.add(slaveHostPort);

            switch (readMode) {
              case SLAVES:
              case MIXED:
              case MIXED_SLAVES:
                final JedisPool slavePool = slavePoolFactory.apply(slaveHostPort);
                slavePools.put(slaveHostPort, slavePool);
                slotSlavePools.add(slavePool);
                break;
              case MASTER:
              default:
                break;
            }
          }

          if (readMode != ReadMode.MASTER) {

            final LoadBalancedPools lbPools =
                new RoundRobinPools(slotSlavePools.toArray(new JedisPool[slotSlavePools.size()]));

            Arrays.fill(slaveSlots, slotBegin, slotEnd, lbPools);
          }
        }

        return new JedisClusterSlotCache(readMode, allDiscoveryHostPorts, masterPools, masterSlots,
            slavePools, slaveSlots, masterPoolFactory, slavePoolFactory);
      } catch (final JedisConnectionException e) {
        // try next discoveryNode...
      }
    }

    return new JedisClusterSlotCache(readMode, allDiscoveryHostPorts, masterPools, masterSlots,
        slavePools, slaveSlots, masterPoolFactory, slavePoolFactory);
  }

  @SuppressWarnings("unchecked")
  void discoverClusterSlots(final Jedis jedis) {

    final int dedupeDiscovery = slotDiscoveryCnt;
    final long writeStamp = lock.writeLock();
    try {

      if (dedupeDiscovery != slotDiscoveryCnt) {
        return;
      }

      Arrays.fill(masterSlots, null);
      Arrays.fill(slaveSlots, null);

      final Set<HostAndPort> staleMasterPools = new HashSet<>(masterPools.keySet());
      final Set<HostAndPort> staleSlavePools = new HashSet<>(slavePools.keySet());

      final List<Object> slots = jedis.clusterSlots();

      for (final Object slotInfoObj : slots) {

        final List<Object> slotInfo = (List<Object>) slotInfoObj;

        final int slotBegin = ((Long) slotInfo.get(0)).intValue();
        final int slotEnd = ((Long) slotInfo.get(1)).intValue();

        switch (defaultReadMode) {
          case MIXED_SLAVES:
          case MIXED:
          case MASTER:
            final HostAndPort masterHostPort =
                generateHostAndPort((List<Object>) slotInfo.get(MASTER_NODE_INDEX));
            discoveryHostPorts.add(masterHostPort);

            final JedisPool masterPool = masterPoolFactory.apply(masterHostPort);
            masterPools.put(masterHostPort, masterPool);

            Arrays.fill(masterSlots, slotBegin, slotEnd, masterPool);
            break;
          case SLAVES:
          default:
            break;
        }

        final ArrayList<JedisPool> slotSlavePools =
            defaultReadMode == ReadMode.MASTER ? null : new ArrayList<>(2);

        for (int i = MASTER_NODE_INDEX + 1, slotInfoSize = slotInfo.size(); i < slotInfoSize; i++) {

          final HostAndPort slaveHostPort = generateHostAndPort((List<Object>) slotInfo.get(i));
          discoveryHostPorts.add(slaveHostPort);

          switch (defaultReadMode) {
            case SLAVES:
            case MIXED:
            case MIXED_SLAVES:
              staleSlavePools.remove(slaveHostPort);

              final JedisPool slavePool =
                  slavePools.computeIfAbsent(slaveHostPort, slavePoolFactory);
              slotSlavePools.add(slavePool);
              break;
            case MASTER:
            default:
              break;
          }
        }

        if (defaultReadMode != ReadMode.MASTER) {

          final LoadBalancedPools lbPools =
              new RoundRobinPools(slotSlavePools.toArray(new JedisPool[slotSlavePools.size()]));

          Arrays.fill(slaveSlots, slotBegin, slotEnd, lbPools);
        }
      }

      staleMasterPools.stream().map(masterPools::remove).filter(Objects::nonNull).forEach(pool -> {
        try {
          pool.destroy();
        } catch (final RuntimeException e) {
          // closing anyways...
        }
      });

      staleSlavePools.stream().map(slavePools::remove).filter(Objects::nonNull).forEach(pool -> {
        try {
          pool.destroy();
        } catch (final RuntimeException e) {
          // closing anyways...
        }
      });

      slotDiscoveryCnt++;
    } finally {
      lock.unlockWrite(writeStamp);
    }
  }

  private static HostAndPort generateHostAndPort(final List<Object> hostInfos) {

    final String ip = RESP.toString(hostInfos.get(0));

    return new HostAndPort(ip.equals("127.0.0.1") || ip.equals("::1") ? "localhost" : ip,
        ((Long) hostInfos.get(1)).intValue());
  }

  Jedis getAskJedis(final HostAndPort askHostPort) {

    switch (defaultReadMode) {
      case MASTER:
      case MIXED:
      case MIXED_SLAVES:
        return getAskJedisModeChecked(askHostPort);
      case SLAVES:
        return new Jedis(askHostPort.getHost(), askHostPort.getPort());
      default:
        return null;
    }
  }

  private Jedis getAskJedisModeChecked(final HostAndPort askHostPort) {

    long readStamp = lock.tryOptimisticRead();

    JedisPool pool = masterPools.get(askHostPort);

    if (!lock.validate(readStamp)) {

      readStamp = lock.readLock();
      try {
        pool = masterPools.get(askHostPort);
      } finally {
        lock.unlockRead(readStamp);
      }
    }

    return pool == null ? new Jedis(askHostPort.getHost(), askHostPort.getPort())
        : pool.getResource();
  }

  JedisPool getSlotPool(final ReadMode readMode, final int slot) {

    switch (defaultReadMode) {
      case MASTER:
      case SLAVES:
        return getSlotPoolModeChecked(defaultReadMode, slot);
      case MIXED:
      case MIXED_SLAVES:
        return getSlotPoolModeChecked(readMode, slot);
      default:
        return null;
    }
  }

  private JedisPool getSlotPoolModeChecked(final ReadMode readMode, final int slot) {

    long readStamp = lock.tryOptimisticRead();

    JedisPool pool = getRoundRobinPool(readMode, slot);

    if (!lock.validate(readStamp)) {

      readStamp = lock.readLock();
      try {
        pool = getRoundRobinPool(readMode, slot);
      } finally {
        lock.unlockRead(readStamp);
      }
    }

    return pool;
  }

  private JedisPool getRoundRobinPool(final ReadMode readMode, final int slot) {

    switch (readMode) {
      case MASTER:
        return masterSlots[slot];
      case MIXED:
      case MIXED_SLAVES:
        LoadBalancedPools lbSlaves = slaveSlots[slot];
        if (lbSlaves == null) {
          return masterSlots[slot];
        }

        final JedisPool slavePool = lbSlaves.getNextPool(readMode);

        return slavePool == null ? masterSlots[slot] : slavePool;
      case SLAVES:
        lbSlaves = slaveSlots[slot];
        if (lbSlaves == null) {
          return masterSlots[slot];
        }

        return lbSlaves.getNextPool(readMode);
      default:
        return null;
    }
  }

  List<JedisPool> getShuffledPools(final ReadMode readMode) {

    switch (defaultReadMode) {
      case MASTER:
      case SLAVES:
        return getShuffledPoolsModeChecked(defaultReadMode);
      case MIXED:
      case MIXED_SLAVES:
        return getShuffledPoolsModeChecked(readMode);
      default:
        return null;
    }
  }

  private List<JedisPool> getShuffledPoolsModeChecked(final ReadMode readMode) {

    List<JedisPool> pools = null;
    switch (readMode) {
      case MASTER:
        pools = getMasterPools();
        break;
      case MIXED:
      case MIXED_SLAVES:
      case SLAVES:
        pools = getAllPools();
        break;
      default:
        return null;
    }

    Collections.shuffle(pools);
    return pools;
  }

  List<JedisPool> getMasterPools() {

    final long readStamp = lock.readLock();
    try {
      return new ArrayList<>(masterPools.values());
    } finally {
      lock.unlockRead(readStamp);
    }
  }

  List<JedisPool> getSlavePools() {

    final long readStamp = lock.readLock();
    try {
      return new ArrayList<>(slavePools.values());
    } finally {
      lock.unlockRead(readStamp);
    }
  }

  List<JedisPool> getAllPools() {

    final long readStamp = lock.readLock();
    try {
      final List<JedisPool> allPools = new ArrayList<>(masterPools.size() + slavePools.size());
      allPools.addAll(masterPools.values());
      allPools.addAll(slavePools.values());
      return allPools;
    } finally {
      lock.unlockRead(readStamp);
    }
  }

  Set<HostAndPort> getDiscoveryHostPorts() {

    return discoveryHostPorts;
  }

  @Override
  public void close() throws IOException {

    final long writeStamp = lock.writeLock();
    try {

      masterPools.forEach((key, pool) -> {
        try {
          if (pool != null) {
            pool.destroy();
          }
        } catch (final Exception e) {
          // closing anyways...
        }
      });

      masterPools.clear();

      slavePools.forEach((key, pool) -> {
        try {
          if (pool != null) {
            pool.destroy();
          }
        } catch (final Exception e) {
          // closing anyways...
        }
      });

      slavePools.clear();
    } finally {
      lock.unlockWrite(writeStamp);
    }
  }
}
