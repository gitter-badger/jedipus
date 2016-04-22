package com.fabahaba.jedipus.cluster;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import com.fabahaba.fava.func.Retryable;
import com.google.common.base.Throwables;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

public interface JedisClusterExecutor extends Retryable {

  public void acceptJedis(final Consumer<JedisCluster> jedisConsumer);

  default void acceptJedis(final Consumer<JedisCluster> jedisConsumer, final int numRetries) {

    retryRun(() -> acceptJedis(jedisConsumer), numRetries);
  }

  public <T> T applyJedis(final Function<JedisCluster, T> jedisFunc);

  default <T> T applyJedis(final Function<JedisCluster, T> jedisFunc, final int numRetries) {

    return retryCallWithThrow(() -> applyJedis(jedisFunc), numRetries);
  }

  default <R> Optional<R> applyJedisOptionalWithThrow(
      final Function<JedisCluster, R> jedisFunction) {

    return Optional.ofNullable(applyJedis(jedisFunction));
  }

  default <R> Optional<R> applyJedisOptionalWithThrow(final Function<JedisCluster, R> jedisFunction,
      final int numRetries) {

    return Optional.ofNullable(applyJedis(jedisFunction, numRetries));
  }

  default <R> Optional<R> applyJedisOptional(final Function<JedisCluster, R> jedisFunction) {

    try {
      return applyJedisOptionalWithThrow(jedisFunction);
    } catch (final JedisConnectionException rce) {
      return Optional.empty();
    }
  }

  default <R> Optional<R> applyJedisOptional(final Function<JedisCluster, R> jedisFunction,
      final int numRetries) {

    try {
      return applyJedisOptionalWithThrow(jedisFunction, numRetries);
    } catch (final JedisConnectionException rce) {
      return Optional.empty();
    }
  }

  @Override
  default void handleException(final Exception ex) {

    if (ex.getCause() != null && ex.getCause() instanceof InterruptedException) {
      throw Throwables.propagate(ex);
    }

    if (ex instanceof JedisDataException) {
      throw ((JedisDataException) ex);
    }

    catching(ex);
  }
}
