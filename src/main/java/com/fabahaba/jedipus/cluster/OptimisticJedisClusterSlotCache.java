package com.fabahaba.jedipus.cluster;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

class OptimisticJedisClusterSlotCache extends JedisClusterSlotCache {

  OptimisticJedisClusterSlotCache(final ReadMode defaultReadMode,
      final Set<HostAndPort> discoveryNodes, final Map<HostAndPort, JedisPool> masterPools,
      final JedisPool[] masterSlots, final Map<HostAndPort, JedisPool> slavePools,
      final LoadBalancedPools[] slaveSlots,
      final Function<HostAndPort, JedisPool> masterPoolFactory,
      final Function<HostAndPort, JedisPool> slavePoolFactory,
      final Function<HostAndPort, Jedis> jedisAskFactory,
      final Function<JedisPool[], LoadBalancedPools> lbFactory, final boolean initReadOnly) {

    super(defaultReadMode, discoveryNodes, masterPools, masterSlots, slavePools, slaveSlots,
        masterPoolFactory, slavePoolFactory, jedisAskFactory, lbFactory, initReadOnly, true);
  }

  @Override
  protected Jedis getAskJedis(final HostAndPort askHostPort) {

    final JedisPool pool = getAskJedisGuarded(askHostPort);

    return pool == null ? jedisAskFactory.apply(askHostPort) : pool.getResource();
  }

  @Override
  protected JedisPool getSlotPoolModeChecked(final ReadMode readMode, final int slot) {

    return getLoadBalancedPool(readMode, slot);
  }

  @Override
  JedisPool getMasterPoolIfPresent(final HostAndPort hostPort) {

    return masterPools.get(hostPort);
  }

  @Override
  JedisPool getSlavePoolIfPresent(final HostAndPort hostPort) {

    return slavePools.get(hostPort);
  }

  @Override
  JedisPool getPoolIfPresent(final HostAndPort hostPort) {

    final JedisPool pool = masterPools.get(hostPort);

    return pool == null ? slavePools.get(hostPort) : pool;
  }
}
