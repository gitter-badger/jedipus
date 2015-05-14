package com.fabahaba.jedipus.cache;

import com.fabahaba.fava.func.Retryable;
import com.fabahaba.fava.serialization.gson.GsonUtils;
import com.fabahaba.jedipus.JedisExecutor;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.gson.Gson;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class RedisHashCache<F, V> implements Retryable {

  public static final int DEFAULT_NUM_CACHE_LOADER_RETRIES = 1;

  private final Gson gson;
  private final JedisExecutor redisPoolExecutor;
  private final String mapName;
  private final ExecutorService executor;
  private final Type fieldType;
  private final Type valueType;
  private final LoadingCache<F, Optional<V>> passiveCache;
  private final int numCacheLoaderRetries;

  public RedisHashCache(final Gson gson, final JedisExecutor redisPoolExecutor,
      final String mapName, final Type fieldType, final Type valueType,
      final CacheBuilder<Object, Object> cacheBuilder) {
    this(gson, redisPoolExecutor, mapName, fieldType, valueType, cacheBuilder,
        DEFAULT_NUM_CACHE_LOADER_RETRIES);
  }

  public RedisHashCache(final Gson gson, final JedisExecutor redisPoolExecutor,
      final String mapName, final Type fieldType, final Type valueType,
      final CacheBuilder<Object, Object> cacheBuilder, final int numCacheLoaderRetries) {
    this(gson, redisPoolExecutor, mapName, fieldType, valueType, cacheBuilder, ForkJoinPool
        .commonPool(), numCacheLoaderRetries);
  }

  public RedisHashCache(final Gson gson, final JedisExecutor redisPoolExecutor,
      final String mapName, final Type fieldType, final Type valueType,
      final CacheBuilder<Object, Object> cacheBuilder, final ExecutorService executor) {
    this(gson, redisPoolExecutor, mapName, fieldType, valueType, cacheBuilder, executor,
        DEFAULT_NUM_CACHE_LOADER_RETRIES);
  }

  public RedisHashCache(final Gson gson, final JedisExecutor redisPoolExecutor,
      final String mapName, final Type fieldType, final Type valueType,
      final CacheBuilder<Object, Object> cacheBuilder, final ExecutorService executor,
      final int numCacheLoaderRetries) {

    this.gson = gson;
    this.redisPoolExecutor = redisPoolExecutor;
    this.mapName = mapName;
    this.executor = executor;
    this.fieldType = fieldType;
    this.valueType = valueType;
    this.passiveCache = cacheBuilder.build(new RedisCacheMapLoader());
    this.numCacheLoaderRetries = numCacheLoaderRetries;
  }

  public Optional<V> get(final F field) {
    return passiveCache.getUnchecked(field);
  }

  public Map<F, Optional<V>> getAll(final Set<F> fields) {
    try {
      return passiveCache.getAll(fields);
    } catch (final ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  public Long put(final F field, final V val) {
    return put(field, val, 1);
  }

  public Long put(final F field, final V val, final int numRetries) {
    return redisPoolExecutor.applyJedis(jedis -> {
      final Long mapFieldExists = jedis.hset(mapName, gson.toJson(field), gson.toJson(val));
      passiveCache.put(field, Optional.of(val));
      return mapFieldExists;
    }, numRetries);
  }

  public void putAll(final Map<F, V> entries) {
    putAll(entries, 1);
  }

  public void putAll(final Map<F, V> entries, final int numRetries) {
    final Map<F, Optional<V>> optionalEntries = new HashMap<>();
    redisPoolExecutor.acceptPipeline(pipeline -> {
      entries.entrySet().forEach(entry -> {
        optionalEntries.put(entry.getKey(), Optional.of(entry.getValue()));
        pipeline.hset(mapName, gson.toJson(entry.getKey()), gson.toJson(entry.getValue()));
      });
    }, numRetries);
    passiveCache.putAll(optionalEntries);
  }

  public synchronized void loadAll() {
    loadAll(1);
  }

  public synchronized void loadAll(final int numRetries) {
    redisPoolExecutor.applyJedisOptional(jedis -> jedis.hgetAll(mapName), numRetries).ifPresent(
        rawMap -> {
          rawMap.forEach((f, v) -> {
            final F field = gson.fromJson(f, fieldType);
            if (field != null) {
              final V val = gson.fromJson(v, valueType);
              passiveCache.put(field, Optional.ofNullable(val));
            }
          });
        });
  }

  public Set<F> getCacheFieldSetView() {
    return passiveCache.asMap().keySet();
  }

  private static String fieldToKey(final Gson gson, final Object field) {
    return GsonUtils.singleEscapeQuotesInJson(gson.toJson(field));
  }

  private class RedisCacheMapLoader extends CacheLoader<F, Optional<V>> {

    @Override
    public Optional<V> load(final F field) throws Exception {
      return getWithTypedField(field);
    }

    @Override
    public ListenableFuture<Optional<V>> reload(final F field, final Optional<V> prevVal) {
      final ListenableFutureTask<Optional<V>> loaderTask =
          ListenableFutureTask.create(() -> getWithTypedField(field));

      if (!prevVal.isPresent()) {
        loaderTask.run();
      } else {
        executor.execute(loaderTask);
      }

      return loaderTask;
    }

    @Override
    public Map<F, Optional<V>> loadAll(final Iterable<? extends F> fields) throws Exception {
      final String[] fieldKeys =
          StreamSupport.stream(fields.spliterator(), false)
              .map(f -> RedisHashCache.fieldToKey(gson, f)).toArray(String[]::new);

      final List<String> response =
          redisPoolExecutor.applyJedisOptional(jedis -> jedis.hmget(mapName, fieldKeys),
              numCacheLoaderRetries).orElse(null);

      if (response == null)
        return StreamSupport.stream(fields.spliterator(), false).collect(
            Collectors.toMap(f -> f, f -> Optional.empty()));

      final Map<F, Optional<V>> resultMap = new HashMap<>();
      int responseIndex = 0;

      for (final F field : fields) {
        final String responseJson = response.get(responseIndex++);
        if (responseJson != null) {
          resultMap.put(field, Optional.ofNullable(gson.fromJson(responseJson, valueType)));
        } else {
          resultMap.put(field, Optional.empty());
        }
      }

      return resultMap;
    }

    private Optional<V> getWithTypedField(final F field) {
      return redisPoolExecutor.applyJedisOptional(
          jedis -> jedis.hget(mapName, RedisHashCache.fieldToKey(gson, field)),
          numCacheLoaderRetries).map(valJson -> gson.fromJson(valJson, valueType));
    }
  }
}
