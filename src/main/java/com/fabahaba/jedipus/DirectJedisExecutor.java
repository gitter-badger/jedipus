package com.fabahaba.jedipus;

import com.google.common.base.MoreObjects;

import redis.clients.jedis.Jedis;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

public class DirectJedisExecutor implements JedisExecutor {

  private final Jedis jedis;

  public DirectJedisExecutor(final Jedis jedis) {

    this.jedis = jedis;
  }

  @Override
  public void acceptJedis(final Consumer<Jedis> jedisConsumer) {

    jedisConsumer.accept(jedis);
  }

  @Override
  public <T> T applyJedis(final Function<Jedis, T> jedisFunc) {

    return jedisFunc.apply(jedis);
  }

  public Jedis getJedis() {
    return this.jedis;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("jedis", jedis).toString();
  }

  @Override
  public boolean equals(final Object other) {
    if (this == other)
      return true;
    if (!(other instanceof DirectJedisExecutor))
      return false;
    final DirectJedisExecutor castOther = (DirectJedisExecutor) other;
    return Objects.equals(jedis, castOther.jedis);
  }

  @Override
  public int hashCode() {
    return Objects.hash(jedis);
  }
}
