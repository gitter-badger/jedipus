package com.fabahaba.jedipus.cluster;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.PipelineBase;
import redis.clients.jedis.exceptions.JedisAskDataException;
import redis.clients.jedis.exceptions.JedisClusterException;
import redis.clients.jedis.exceptions.JedisClusterMaxRedirectionsException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisMovedDataException;
import redis.clients.jedis.exceptions.JedisRedirectionException;
import redis.clients.util.JedisClusterCRC16;

public class JedisClusterExecutor implements Closeable {

  private static final int DEFAULT_TIMEOUT = 2000;
  private static final int DEFAULT_MAX_REDIRECTIONS = 5;
  private static final int DEFAULT_MAX_RETRIES = 2;
  private static final int DEFAULT_TRY_RANDOM_AFTER = 1;

  private final int maxRedirections;
  private final int maxRetries;
  private final int tryRandomAfter;

  private final JedisClusterConnHandler connectionHandler;

  public JedisClusterExecutor(final Collection<HostAndPort> discoveryNodes) {

    this(discoveryNodes, DEFAULT_TIMEOUT);
  }

  public JedisClusterExecutor(final Collection<HostAndPort> discoveryNodes, final int timeout) {

    this(discoveryNodes, timeout, DEFAULT_MAX_REDIRECTIONS, DEFAULT_MAX_RETRIES,
        DEFAULT_TRY_RANDOM_AFTER, new GenericObjectPoolConfig());
  }

  public JedisClusterExecutor(final Collection<HostAndPort> discoveryNodes, final int timeout,
      final int maxRedirections, final int maxRetries, final int tryRandomAfter,
      final GenericObjectPoolConfig poolConfig) {

    this(discoveryNodes, timeout, timeout, maxRedirections, maxRetries, tryRandomAfter, poolConfig);
  }

  public JedisClusterExecutor(final Collection<HostAndPort> discoveryNodes,
      final int connectionTimeout, final int soTimeout, final int maxRedirections,
      final int maxRetries, final int tryRandomAfter, final GenericObjectPoolConfig poolConfig) {

    this(discoveryNodes, maxRedirections, maxRetries, tryRandomAfter,
        node -> new JedisPool(poolConfig, node.getHost(), node.getPort(), connectionTimeout,
            soTimeout, null, 0, null));
  }

  public JedisClusterExecutor(final Collection<HostAndPort> discoveryNodes,
      final int maxRedirections, final int maxRetries, final int tryRandomAfter,
      final Function<HostAndPort, JedisPool> jedisPoolFactory) {

    this.connectionHandler = new JedisClusterConnHandler(discoveryNodes, jedisPoolFactory);
    this.maxRedirections = maxRedirections;
    this.maxRetries = maxRetries;
    this.tryRandomAfter = tryRandomAfter;
  }

  public int getMaxRedirections() {
    return maxRedirections;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public void acceptJedis(final byte[] slotKey, final Consumer<Jedis> jedisConsumer) {

    acceptJedis(slotKey, jedisConsumer, maxRetries);
  }

  public void acceptJedis(final int slot, final Consumer<Jedis> jedisConsumer) {

    acceptJedis(slot, jedisConsumer, maxRetries);
  }

  public void acceptJedis(final byte[] slotKey, final Consumer<Jedis> jedisConsumer,
      final int maxRetries) {

    acceptJedis(JedisClusterCRC16.getSlot(slotKey), jedisConsumer, maxRetries);
  }

  public void acceptJedis(final int slot, final Consumer<Jedis> jedisConsumer,
      final int maxRetries) {

    applyJedis(slot, j -> {
      jedisConsumer.accept(j);
      return null;
    }, maxRetries);
  }

  public <R> R applyJedis(final byte[] slotKey, final Function<Jedis, R> jedisConsumer) {

    return applyJedis(slotKey, jedisConsumer, maxRetries);
  }

  public <R> R applyJedis(final int slot, final Function<Jedis, R> jedisConsumer) {

    return applyJedis(slot, jedisConsumer, maxRetries);
  }

  public <R> R applyJedis(final byte[] slotKey, final Function<Jedis, R> jedisConsumer,
      final int maxRetries) {

    return applyJedis(JedisClusterCRC16.getSlot(slotKey), jedisConsumer, maxRetries);
  }

  public <R> R applyPipeline(final byte[] slotKey, final Function<Pipeline, R> pipelineConsumer) {

    return applyPipeline(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  public <R> R applyPipeline(final int slot, final Function<Pipeline, R> pipelineConsumer) {

    return applyPipeline(slot, pipelineConsumer, maxRetries);
  }

  public <R> R applyPipeline(final byte[] slotKey, final Function<Pipeline, R> pipelineConsumer,
      final int maxRetries) {

    return applyPipeline(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  public <R> R applyPipeline(final int slot, final Function<Pipeline, R> pipelineConsumer,
      final int maxRetries) {

    return applyJedis(slot, jedis -> {
      final Pipeline pipeline = jedis.pipelined();
      final R result = pipelineConsumer.apply(pipeline);
      pipeline.sync();
      return result;
    }, maxRetries);
  }

  public void acceptPipeline(final byte[] slotKey, final Consumer<PipelineBase> pipelineConsumer) {

    acceptPipeline(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  public void acceptPipeline(final int slot, final Consumer<PipelineBase> pipelineConsumer) {

    acceptPipeline(slot, pipelineConsumer, maxRetries);
  }

  public void acceptPipeline(final byte[] slotKey, final Consumer<PipelineBase> pipelineConsumer,
      final int maxRetries) {

    acceptPipeline(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  public void acceptPipeline(final int slot, final Consumer<PipelineBase> pipelineConsumer,
      final int maxRetries) {

    applyJedis(slot, jedis -> {
      final Pipeline pipeline = jedis.pipelined();
      pipelineConsumer.accept(pipeline);
      pipeline.sync();
      return null;
    }, maxRetries);
  }

  public <R> R applyPipelinedTransaction(final byte[] slotKey,
      final Function<Pipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  public <R> R applyPipelinedTransaction(final int slot,
      final Function<Pipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(slot, pipelineConsumer, maxRetries);
  }

  public <R> R applyPipelinedTransaction(final byte[] slotKey,
      final Function<Pipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipelinedTransaction(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  public <R> R applyPipelinedTransaction(final int slot,
      final Function<Pipeline, R> pipelineConsumer, final int maxRetries) {

    return applyJedis(slot, jedis -> {
      final Pipeline pipeline = jedis.pipelined();
      pipeline.multi();
      final R result = pipelineConsumer.apply(pipeline);
      pipeline.exec();
      pipeline.sync();
      return result;
    }, maxRetries);
  }

  public void acceptPipelinedTransaction(final byte[] slotKey,
      final Consumer<PipelineBase> pipelineConsumer) {

    acceptPipelinedTransaction(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  public void acceptPipelinedTransaction(final int slot,
      final Consumer<PipelineBase> pipelineConsumer) {

    acceptPipelinedTransaction(slot, pipelineConsumer, maxRetries);
  }

  public void acceptPipelinedTransaction(final byte[] slotKey,
      final Consumer<PipelineBase> pipelineConsumer, final int maxRetries) {

    acceptPipelinedTransaction(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  public void acceptPipelinedTransaction(final int slot,
      final Consumer<PipelineBase> pipelineConsumer, final int maxRetries) {

    applyJedis(slot, jedis -> {
      final Pipeline pipeline = jedis.pipelined();
      pipeline.multi();
      pipelineConsumer.accept(pipeline);
      pipeline.exec();
      pipeline.sync();
      return null;
    }, maxRetries);
  }

  @SuppressWarnings("resource")
  public <R> R applyJedis(final int slot, final Function<Jedis, R> jedisConsumer,
      final int maxRetries) {

    Jedis asking = null;
    int retries = 0;
    try {

      // Optimistic first try
      Jedis jedis = null;
      try {
        jedis = connectionHandler.getConnectionFromSlot(slot);
        return jedisConsumer.apply(jedis);
      } catch (final JedisConnectionException jce) {

        if (maxRetries == 0) {
          throw jce;
        }
        retries = 1;
      } catch (final JedisMovedDataException jre) {

        if (jedis == null) {
          connectionHandler.renewSlotCache();
        } else {
          connectionHandler.renewSlotCache(jedis);
        }
      } catch (final JedisAskDataException jre) {

        asking = connectionHandler.getConnectionFromNode(jre.getTargetNode());
      } catch (final JedisRedirectionException jre) {

        throw new JedisClusterException(jre);
      } finally {
        if (jedis != null) {
          jedis.close();
        }
      }

      for (int redirections = retries == 0 ? 1 : 0;;) {

        Jedis connection = null;
        try {

          if (asking == null) {

            connection = retries > tryRandomAfter ? connectionHandler.getConnection()
                : connectionHandler.getConnectionFromSlot(slot);
            return jedisConsumer.apply(connection);
          }

          connection = asking;
          asking = null;
          connection.asking();
          return jedisConsumer.apply(connection);
        } catch (final JedisConnectionException jce) {

          if (++retries > maxRetries) {
            throw jce;
          }
          continue;
        } catch (final JedisMovedDataException jre) {

          if (++redirections > maxRedirections) {
            throw new JedisClusterMaxRedirectionsException(jre);
          }

          if (connection == null) {
            connectionHandler.renewSlotCache();
          } else {
            connectionHandler.renewSlotCache(connection);
          }
          continue;
        } catch (final JedisAskDataException jre) {

          if (++redirections > maxRedirections) {
            throw new JedisClusterMaxRedirectionsException(jre);
          }

          asking = connectionHandler.getConnectionFromNode(jre.getTargetNode());
          continue;
        } catch (final JedisRedirectionException jre) {

          throw new JedisClusterException(jre);
        } finally {
          if (connection != null) {
            connection.close();
          }
        }
      }
    } finally {
      if (asking != null) {
        asking.close();
      }
    }
  }

  public void acceptAllMasters(final Consumer<Jedis> jedisConsumer) {

    acceptAllMasters(jedisConsumer, maxRetries);
  }

  public void acceptAllMasters(final Consumer<Jedis> jedisConsumer, final int maxRetries) {

    JedisConnectionException failure = null;

    for (final JedisPool pool : connectionHandler.getPools()) {

      for (int retries = 0;;) {

        try (Jedis jedis = pool.getResource()) {

          jedisConsumer.accept(jedis);
          break;
        } catch (final JedisConnectionException jce) {

          if (++retries > maxRetries) {
            failure = jce;
            break;
          }

          continue;
        }
      }
    }

    if (failure != null) {
      throw failure;
    }
  }

  @Override
  public void close() throws IOException {

    connectionHandler.close();
  }
}
