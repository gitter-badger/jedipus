package com.fabahaba.jedipus.cluster;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.base.MoreObjects;

import redis.clients.jedis.JedisCluster;

public class DirectJedisClusterExecutor implements JedisClusterExecutor {

  private final JedisCluster jedis;

  public DirectJedisClusterExecutor(final JedisCluster jedis) {

    this.jedis = jedis;
  }

  @Override
  public void acceptJedis(final Consumer<JedisCluster> jedisConsumer) {

    jedisConsumer.accept(jedis);
  }

  @Override
  public <T> T applyJedis(final Function<JedisCluster, T> jedisFunc) {

    return jedisFunc.apply(jedis);
  }

  @Override
  public boolean equals(final Object other) {
    if (this == other) {
      return true;
    }
    if (other == null) {
      return false;
    }
    if (!getClass().equals(other.getClass())) {
      return false;
    }
    final DirectJedisClusterExecutor castOther = DirectJedisClusterExecutor.class.cast(other);
    return Objects.equals(jedis, castOther.jedis);
  }

  @Override
  public int hashCode() {
    return Objects.hash(jedis);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("jedis", jedis).toString();
  }
}
