package com.fabahaba.jedipus;

import java.util.Collection;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

public class ExtendedJedisPoolConfig extends JedisPoolConfig {

  public static final int DEFAULT_MAX_CONSECUTIVE_FAILURES = 3;

  private int connectionTimeoutMillis = Protocol.DEFAULT_TIMEOUT;
  private int maxConsecutiveFailures = DEFAULT_MAX_CONSECUTIVE_FAILURES;

  public static ExtendedJedisPoolConfig getDefaultConfig() {

    return new ExtendedJedisPoolConfig();
  }

  public JedisSentinelPoolExecutor buildExecutor(final String masterName, final int db,
      final Collection<String> sentinelHostPorts, final String password) {

    return buildExecutor(masterName, db, sentinelHostPorts, password, PoolFactory.DEFAULT_FACTORY);
  }

  public JedisSentinelPoolExecutor buildExecutor(final String masterName, final int db,
      final Collection<String> sentinelHostPorts, final String password,
      final PoolFactory poolFactory) {

    return new JedisSentinelPoolExecutor(masterName, db, sentinelHostPorts, password, this,
        poolFactory);
  }

  public ExtendedJedisPoolConfig copy() {

    return new ExtendedJedisPoolConfig().withBlockWhenExhausted(getBlockWhenExhausted())
        .withEvictionPolicyClassName(getEvictionPolicyClassName()).withJmxEnabled(getJmxEnabled())
        .withJmxNamePrefix(getJmxNamePrefix()).withLifo(getLifo()).withFairness(getFairness())
        .withMaxIdle(getMaxIdle()).withMaxTotal(getMaxTotal()).withMaxWaitMillis(getMaxWaitMillis())
        .withMinEvictableIdleTimeMillis(getMinEvictableIdleTimeMillis()).withMinIdle(getMinIdle())
        .withNumTestsPerEvictionRun(getNumTestsPerEvictionRun())
        .withSoftMinEvictableIdleTimeMillis(getSoftMinEvictableIdleTimeMillis())
        .withTestOnCreate(getTestOnCreate()).withTestOnBorrow(getTestOnBorrow())
        .withTestOnReturn(getTestOnReturn()).withTestWhileIdle(getTestWhileIdle())
        .withTimeBetweenEvictionRunsMillis(getTimeBetweenEvictionRunsMillis());
  }

  /**
   *
   * @param maxConsecutiveFailures The maximum number of consecutive failures before re-constructing
   *        the underlying sentinel pool.
   * 
   * @return this ExtendedJedisPoolConfig.
   */
  public ExtendedJedisPoolConfig withMaxConsecutiveFailures(final int maxConsecutiveFailures) {

    this.maxConsecutiveFailures = maxConsecutiveFailures;
    return this;
  }

  /**
   * The maximum number of consecutive failures before re-constructing the underlying sentinel pool.
   *
   * @return {@code maxConsecutiveFailures}
   */
  public int getMaxConsecutiveFailures() {

    return maxConsecutiveFailures;
  }

  public int getConnectionTimeoutMillis() {

    return connectionTimeoutMillis;
  }

  public ExtendedJedisPoolConfig withConnectionTimeoutMillis(final int connectionTimeoutMillis) {

    this.connectionTimeoutMillis = connectionTimeoutMillis;
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

  public ExtendedJedisPoolConfig withFairness(final boolean fairness) {

    setFairness(fairness);
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

  public ExtendedJedisPoolConfig withTestOnCreate(final boolean testOnCreate) {

    setTestOnCreate(testOnCreate);
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
