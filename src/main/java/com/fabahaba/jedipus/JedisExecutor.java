package com.fabahaba.jedipus;

import com.fabahaba.fava.func.Retryable;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public interface JedisExecutor extends Retryable {

  default String getMasterHostPort() {
    return applyJedis(jedis -> jedis.getClient().getHost());
  }

  default void acceptPipeline(final Consumer<Pipeline> pipelineConsumer) {
    acceptJedis(jedis -> {
      final Pipeline pipeline = jedis.pipelined();
      pipelineConsumer.accept(pipeline);
      pipeline.sync();
    });
  }

  default void acceptPipeline(final Consumer<Pipeline> pipelineConsumer, final int numRetries) {
    retryRun(() -> acceptPipeline(pipelineConsumer), numRetries);
  }

  default void acceptPipelinedTransaction(final Consumer<Pipeline> pipelineConsumer) {
    acceptJedis(jedis -> {
      final Pipeline pipeline = jedis.pipelined();
      pipeline.multi();
      pipelineConsumer.accept(pipeline);
      pipeline.exec();
      pipeline.sync();
    });
  }

  default void acceptPipelinedTransaction(final Consumer<Pipeline> pipelineConsumer,
      final int numRetries) {
    retryRun(() -> acceptPipelinedTransaction(pipelineConsumer), numRetries);
  }

  public void acceptJedis(final Consumer<Jedis> jedisConsumer);

  default void acceptJedis(final Consumer<Jedis> jedisConsumer, final int numRetries) {
    retryRun(() -> acceptJedis(jedisConsumer), numRetries);
  }

  default <T> T applyPipeline(final Function<Pipeline, T> pipelineConsumer) {
    return applyJedis(jedis -> {
      final Pipeline pipeline = jedis.pipelined();
      final T result = pipelineConsumer.apply(pipeline);
      pipeline.sync();
      return result;
    });
  }

  default <T> T applyPipeline(final Function<Pipeline, T> pipelineConsumer, final int numRetries) {
    return retryCallWithThrow(() -> applyPipeline(pipelineConsumer), numRetries);
  }

  default <T> T applyPipelinedTransaction(final Function<Pipeline, T> pipelineConsumer) {
    return applyJedis(jedis -> {
      final Pipeline pipeline = jedis.pipelined();
      pipeline.multi();
      final T result = pipelineConsumer.apply(pipeline);
      pipeline.exec();
      pipeline.sync();
      return result;
    });
  }

  default <T> T applyPipelinedTransaction(final Function<Pipeline, T> pipelineConsumer,
      final int numRetries) {
    return retryCallWithThrow(() -> applyPipelinedTransaction(pipelineConsumer), numRetries);
  }

  public <T> T applyJedis(final Function<Jedis, T> jedisFunc);

  default <T> T applyJedis(final Function<Jedis, T> jedisFunc, final int numRetries) {
    return retryCallWithThrow(() -> applyJedis(jedisFunc), numRetries);
  }

  default <R> Optional<R> applyJedisOptionalWithThrow(final Function<Jedis, R> jedisFunction) {
    return Optional.ofNullable(applyJedis(jedisFunction));
  }

  default <R> Optional<R> applyJedisOptionalWithThrow(final Function<Jedis, R> jedisFunction,
      final int numRetries) {
    return Optional.ofNullable(applyJedis(jedisFunction, numRetries));
  }

  default <R> Optional<R> applyJedisOptional(final Function<Jedis, R> jedisFunction) {
    try {
      return applyJedisOptionalWithThrow(jedisFunction);
    } catch (final JedisConnectionException rce) {
      return Optional.empty();
    }
  }

  default <R> Optional<R> applyJedisOptional(final Function<Jedis, R> jedisFunction,
      final int numRetries) {
    try {
      return applyJedisOptionalWithThrow(jedisFunction, numRetries);
    } catch (final JedisConnectionException rce) {
      return Optional.empty();
    }
  }

  static final String REDIS_CURSOR_SENTINEL = "0";

  default void applyOptionalKeyScan(final ScanParams scanParams,
      final Function<List<String>, Boolean> keyConsumer, final int numRetriesPerCall) {
    applyOptionalKeyScan(REDIS_CURSOR_SENTINEL, scanParams, keyConsumer, numRetriesPerCall);
  }

  default void applyOptionalKeyScan(final String startingCursor, final ScanParams scanParams,
      final Function<List<String>, Boolean> keyConsumer, final int numRetriesPerCall) {
    String cursor = startingCursor;
    do {
      final String finalForLambdaCursor = cursor;
      cursor =
          applyJedisOptional(jedis -> jedis.scan(finalForLambdaCursor, scanParams),
              numRetriesPerCall).map(
              scanResult -> keyConsumer.apply(scanResult.getResult()) ? scanResult
                  .getStringCursor() : REDIS_CURSOR_SENTINEL).orElse(REDIS_CURSOR_SENTINEL);
    } while (!cursor.equals(REDIS_CURSOR_SENTINEL));
  }

  default void applyOptionalKeyValueScan(final ScanParams scanParams,
      final BiFunction<String, String, Boolean> keyValueConsumer, final int numRetries) {
    applyOptionalKeyScan(scanParams, keys -> {
      if (keys.isEmpty())
        return Boolean.TRUE;

      final int numKeys = keys.size();
      final String[] keyArray = keys.toArray(new String[numKeys]);

      return applyJedisOptional(jedis -> jedis.mget(keyArray), numRetries).map(keyValues -> {
        for (int i = 0; i < numKeys; i++) {
          if (!keyValueConsumer.apply(keys.get(i), keyValues.get(i)))
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
      }).orElse(Boolean.FALSE);
    }, numRetries);
  }
}
