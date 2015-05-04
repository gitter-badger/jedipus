package com.fabahaba.jedipus;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

public class ExtendedJedisPoolConfig extends JedisPoolConfig {

  private int maxConsecutiveConnectionFailures = 5;
  private int connectionTimeoutMillis = Protocol.DEFAULT_TIMEOUT;
  private int socketTimeoutMillis = Protocol.DEFAULT_TIMEOUT;

  public ExtendedJedisPoolConfig() {
    super();
  }

  public ExtendedJedisPoolConfig withMaxConsecutiveConnectionFailures(
      final int maxConsecutiveConnectionFailures) {
    this.maxConsecutiveConnectionFailures = maxConsecutiveConnectionFailures;
    return this;
  }

  public int getMaxConsecutiveConnectionFailures() {
    return maxConsecutiveConnectionFailures;
  }

  public int getConnectionTimeoutMillis() {
    return connectionTimeoutMillis;
  }

  public ExtendedJedisPoolConfig withConnectionTimeoutMillis(final int connectionTimeoutMillis) {
    this.connectionTimeoutMillis = connectionTimeoutMillis;
    return this;
  }

  public int getSocketTimeoutMillis() {
    return socketTimeoutMillis;
  }

  public ExtendedJedisPoolConfig withSocketTimeoutMillis(final int socketTimeoutMillis) {
    this.socketTimeoutMillis = socketTimeoutMillis;
    return this;
  }

  public ExtendedJedisPoolConfig withMaxTotal(final int maxTotal) {
    setMaxTotal(maxTotal);
    return this;
  }

  public ExtendedJedisPoolConfig withMaxIdle(final int maxIdle) {
    setMaxIdle(maxIdle);
    return this;
  }

  public ExtendedJedisPoolConfig withMinIdle(final int minIdle) {
    setMinIdle(minIdle);
    return this;
  }

  public ExtendedJedisPoolConfig withLifo(final boolean lifo) {
    setLifo(lifo);
    return this;
  }

  public ExtendedJedisPoolConfig withMaxWaitMillis(final long maxWaitMillis) {
    setMaxWaitMillis(maxWaitMillis);
    return this;
  }

  public ExtendedJedisPoolConfig withMinEvictableIdleTimeMillis(
      final long minEvictableIdleTimeMillis) {
    setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
    return this;
  }

  public ExtendedJedisPoolConfig withSoftMinEvictableIdleTimeMillis(
      final long softMinEvictableIdleTimeMillis) {
    setSoftMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis);
    return this;
  }

  public ExtendedJedisPoolConfig withNumTestsPerEvictionRun(final int numTestsPerEvictionRun) {
    setNumTestsPerEvictionRun(numTestsPerEvictionRun);
    return this;
  }

  public ExtendedJedisPoolConfig withTestOnBorrow(final boolean testOnBorrow) {
    setTestOnBorrow(testOnBorrow);
    return this;
  }

  public ExtendedJedisPoolConfig withTestOnReturn(final boolean testOnReturn) {
    setTestOnReturn(testOnReturn);
    return this;
  }

  public ExtendedJedisPoolConfig withTestWhileIdle(final boolean testWhileIdle) {
    setTestWhileIdle(testWhileIdle);
    return this;
  }

  public ExtendedJedisPoolConfig withTimeBetweenEvictionRunsMillis(
      final long timeBetweenEvictionRunsMillis) {
    setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
    return this;
  }

  public ExtendedJedisPoolConfig withEvictionPolicyClassName(final String evictionPolicyClassName) {
    setEvictionPolicyClassName(evictionPolicyClassName);
    return this;
  }

  public ExtendedJedisPoolConfig withBlockWhenExhausted(final boolean blockWhenExhausted) {
    setBlockWhenExhausted(blockWhenExhausted);
    return this;
  }

  public ExtendedJedisPoolConfig withJmxEnabled(final boolean jmxEnabled) {
    setJmxEnabled(jmxEnabled);
    return this;
  }

  public ExtendedJedisPoolConfig withJmxNamePrefix(final String jmxNamePrefix) {
    setJmxNamePrefix(jmxNamePrefix);
    return this;
  }
}
