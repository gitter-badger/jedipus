package com.fabahaba.jedipus.cluster;

import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;

import redis.clients.jedis.JedisPool;

public interface LoadBalancedPools {

  JedisPool next(final ReadMode readMode);
}
