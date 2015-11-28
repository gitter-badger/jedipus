package com.fabahaba.jedipus;

import org.apache.commons.pool2.ObjectPool;

import redis.clients.jedis.Jedis;

import java.util.Set;

@FunctionalInterface
public interface PoolFactory {

  static final PoolFactory DEFAULT_FACTORY = (masterName, db, sentinelHostPorts, password,
      poolConfig) -> JedisPoolFactory.createMasterPool(masterName, sentinelHostPorts, poolConfig,
          poolConfig.getConnectionTimeoutMillis(), poolConfig.getConnectionTimeoutMillis(),
          password, 0, PoolFactory.class.getName(), hostPort -> hostPort);

  public ObjectPool<Jedis> newPool(final String masterName, final int db,
      final Set<String> sentinelHostPorts, final String password,
      final ExtendedJedisPoolConfig poolConfig);
}
