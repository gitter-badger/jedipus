package com.fabahaba.jedipus;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.util.Pool;

import java.util.Set;

@FunctionalInterface
public interface PoolFactory {

  static final PoolFactory DEFAULT_FACTORY = (masterName, db, sentinelHostPorts, password,
      poolConfig) -> new JedisSentinelPool(masterName, sentinelHostPorts, poolConfig,
      poolConfig.getConnectionTimeoutMillis(), password, db);

  public Pool<Jedis> newPool(final String masterName, final int db,
      final Set<String> sentinelHostPorts, final String password,
      final ExtendedJedisPoolConfig poolConfig);
}
