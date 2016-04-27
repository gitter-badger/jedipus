package com.fabahaba.jedipus.cluster;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
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

public final class JedisClusterExecutor implements Closeable {

  public static enum ReadMode {
    MASTER, SLAVES, MIXED, MIXED_SLAVES;
  }

  private static final int DEFAULT_TIMEOUT = 2000;
  private static final int DEFAULT_MAX_REDIRECTIONS = 5;
  private static final int DEFAULT_MAX_RETRIES = 2;
  private static final int DEFAULT_TRY_RANDOM_AFTER = 1;

  private final int maxRedirections;
  private final int maxRetries;
  private final int tryRandomAfter;

  private final JedisClusterConnHandler connHandler;

  public static Builder startBuilding() {

    return new Builder();
  }

  private JedisClusterExecutor(final ReadMode defaultReadMode,
      final Collection<HostAndPort> discoveryHostPorts, final int maxRedirections,
      final int maxRetries, final int tryRandomAfter,
      final Function<HostAndPort, JedisPool> masterPoolFactory,
      final Function<HostAndPort, JedisPool> slavePoolFactory,
      final Function<JedisPool[], LoadBalancedPools> lbFactory, final boolean initReadOnly) {

    this.connHandler = new JedisClusterConnHandler(defaultReadMode, discoveryHostPorts,
        masterPoolFactory, slavePoolFactory, lbFactory, initReadOnly);

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

  public void acceptJedis(final ReadMode readMode, final byte[] slotKey,
      final Consumer<Jedis> jedisConsumer) {

    acceptJedis(readMode, slotKey, jedisConsumer, maxRetries);
  }

  public void acceptJedis(final int slot, final Consumer<Jedis> jedisConsumer) {

    acceptJedis(slot, jedisConsumer, maxRetries);
  }

  public void acceptJedis(final ReadMode readMode, final int slot,
      final Consumer<Jedis> jedisConsumer) {

    acceptJedis(readMode, slot, jedisConsumer, maxRetries);
  }

  public void acceptJedis(final byte[] slotKey, final Consumer<Jedis> jedisConsumer,
      final int maxRetries) {

    acceptJedis(JedisClusterCRC16.getSlot(slotKey), jedisConsumer, maxRetries);
  }

  public void acceptJedis(final ReadMode readMode, final byte[] slotKey,
      final Consumer<Jedis> jedisConsumer, final int maxRetries) {

    acceptJedis(readMode, JedisClusterCRC16.getSlot(slotKey), jedisConsumer, maxRetries);
  }

  public void acceptJedis(final int slot, final Consumer<Jedis> jedisConsumer,
      final int maxRetries) {

    acceptJedis(connHandler.getDefaultReadMode(), slot, jedisConsumer, maxRetries);
  }

  public void acceptJedis(final ReadMode readMode, final int slot,
      final Consumer<Jedis> jedisConsumer, final int maxRetries) {

    applyJedis(readMode, slot, j -> {
      jedisConsumer.accept(j);
      return null;
    }, maxRetries);
  }

  public <R> R applyJedis(final byte[] slotKey, final Function<Jedis, R> jedisConsumer) {

    return applyJedis(slotKey, jedisConsumer, maxRetries);
  }

  public <R> R applyJedis(final ReadMode readMode, final byte[] slotKey,
      final Function<Jedis, R> jedisConsumer) {

    return applyJedis(readMode, slotKey, jedisConsumer, maxRetries);
  }

  public <R> R applyJedis(final int slot, final Function<Jedis, R> jedisConsumer) {

    return applyJedis(slot, jedisConsumer, maxRetries);
  }

  public <R> R applyJedis(final ReadMode readMode, final int slot,
      final Function<Jedis, R> jedisConsumer) {

    return applyJedis(readMode, slot, jedisConsumer, maxRetries);
  }

  public <R> R applyJedis(final byte[] slotKey, final Function<Jedis, R> jedisConsumer,
      final int maxRetries) {

    return applyJedis(JedisClusterCRC16.getSlot(slotKey), jedisConsumer, maxRetries);
  }

  public <R> R applyJedis(final ReadMode readMode, final byte[] slotKey,
      final Function<Jedis, R> jedisConsumer, final int maxRetries) {

    return applyJedis(readMode, JedisClusterCRC16.getSlot(slotKey), jedisConsumer, maxRetries);
  }

  public <R> R applyPipeline(final byte[] slotKey, final Function<Pipeline, R> pipelineConsumer) {

    return applyPipeline(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  public <R> R applyPipeline(final ReadMode readMode, final byte[] slotKey,
      final Function<Pipeline, R> pipelineConsumer) {

    return applyPipeline(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  public <R> R applyPipeline(final int slot, final Function<Pipeline, R> pipelineConsumer) {

    return applyPipeline(slot, pipelineConsumer, maxRetries);
  }

  public <R> R applyPipeline(final ReadMode readMode, final int slot,
      final Function<Pipeline, R> pipelineConsumer) {

    return applyPipeline(readMode, slot, pipelineConsumer, maxRetries);
  }

  public <R> R applyPipeline(final byte[] slotKey, final Function<Pipeline, R> pipelineConsumer,
      final int maxRetries) {

    return applyPipeline(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  public <R> R applyPipeline(final ReadMode readMode, final byte[] slotKey,
      final Function<Pipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipeline(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  public <R> R applyPipeline(final int slot, final Function<Pipeline, R> pipelineConsumer,
      final int maxRetries) {

    return applyPipeline(connHandler.getDefaultReadMode(), slot, pipelineConsumer, maxRetries);
  }

  public <R> R applyPipeline(final ReadMode readMode, final int slot,
      final Function<Pipeline, R> pipelineConsumer, final int maxRetries) {

    return applyJedis(readMode, slot, jedis -> {
      final Pipeline pipeline = jedis.pipelined();
      final R result = pipelineConsumer.apply(pipeline);
      pipeline.sync();
      return result;
    }, maxRetries);
  }

  public void acceptPipeline(final byte[] slotKey, final Consumer<PipelineBase> pipelineConsumer) {

    acceptPipeline(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  public void acceptPipeline(final ReadMode readMode, final byte[] slotKey,
      final Consumer<PipelineBase> pipelineConsumer) {

    acceptPipeline(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  public void acceptPipeline(final int slot, final Consumer<PipelineBase> pipelineConsumer) {

    acceptPipeline(slot, pipelineConsumer, maxRetries);
  }

  public void acceptPipeline(final ReadMode readMode, final int slot,
      final Consumer<PipelineBase> pipelineConsumer) {

    acceptPipeline(readMode, slot, pipelineConsumer, maxRetries);
  }

  public void acceptPipeline(final byte[] slotKey, final Consumer<PipelineBase> pipelineConsumer,
      final int maxRetries) {

    acceptPipeline(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  public void acceptPipeline(final ReadMode readMode, final byte[] slotKey,
      final Consumer<PipelineBase> pipelineConsumer, final int maxRetries) {

    acceptPipeline(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  public void acceptPipeline(final int slot, final Consumer<PipelineBase> pipelineConsumer,
      final int maxRetries) {

    acceptPipeline(connHandler.getDefaultReadMode(), slot, pipelineConsumer, maxRetries);
  }

  public void acceptPipeline(final ReadMode readMode, final int slot,
      final Consumer<PipelineBase> pipelineConsumer, final int maxRetries) {

    applyJedis(readMode, slot, jedis -> {
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

  public <R> R applyPipelinedTransaction(final ReadMode readMode, final byte[] slotKey,
      final Function<Pipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  public <R> R applyPipelinedTransaction(final int slot,
      final Function<Pipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(slot, pipelineConsumer, maxRetries);
  }

  public <R> R applyPipelinedTransaction(final ReadMode readMode, final int slot,
      final Function<Pipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(readMode, slot, pipelineConsumer, maxRetries);
  }

  public <R> R applyPipelinedTransaction(final byte[] slotKey,
      final Function<Pipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipelinedTransaction(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  public <R> R applyPipelinedTransaction(final ReadMode readMode, final byte[] slotKey,
      final Function<Pipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipelinedTransaction(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  public <R> R applyPipelinedTransaction(final int slot,
      final Function<Pipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipelinedTransaction(connHandler.getDefaultReadMode(), slot, pipelineConsumer,
        maxRetries);
  }

  public <R> R applyPipelinedTransaction(final ReadMode readMode, final int slot,
      final Function<Pipeline, R> pipelineConsumer, final int maxRetries) {

    return applyJedis(readMode, slot, jedis -> {
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

  public void acceptPipelinedTransaction(final ReadMode readMode, final byte[] slotKey,
      final Consumer<PipelineBase> pipelineConsumer) {

    acceptPipelinedTransaction(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  public void acceptPipelinedTransaction(final int slot,
      final Consumer<PipelineBase> pipelineConsumer) {

    acceptPipelinedTransaction(slot, pipelineConsumer, maxRetries);
  }

  public void acceptPipelinedTransaction(final ReadMode readMode, final int slot,
      final Consumer<PipelineBase> pipelineConsumer) {

    acceptPipelinedTransaction(readMode, slot, pipelineConsumer, maxRetries);
  }

  public void acceptPipelinedTransaction(final byte[] slotKey,
      final Consumer<PipelineBase> pipelineConsumer, final int maxRetries) {

    acceptPipelinedTransaction(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  public void acceptPipelinedTransaction(final ReadMode readMode, final byte[] slotKey,
      final Consumer<PipelineBase> pipelineConsumer, final int maxRetries) {

    acceptPipelinedTransaction(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  public void acceptPipelinedTransaction(final int slot,
      final Consumer<PipelineBase> pipelineConsumer, final int maxRetries) {

    acceptPipelinedTransaction(connHandler.getDefaultReadMode(), slot, pipelineConsumer,
        maxRetries);
  }

  public void acceptPipelinedTransaction(final ReadMode readMode, final int slot,
      final Consumer<PipelineBase> pipelineConsumer, final int maxRetries) {

    applyJedis(readMode, slot, jedis -> {
      final Pipeline pipeline = jedis.pipelined();
      pipeline.multi();
      pipelineConsumer.accept(pipeline);
      pipeline.exec();
      pipeline.sync();
      return null;
    }, maxRetries);
  }

  public <R> R applyJedis(final int slot, final Function<Jedis, R> jedisConsumer,
      final int maxRetries) {

    return applyJedis(connHandler.getDefaultReadMode(), slot, jedisConsumer, maxRetries);
  }

  @SuppressWarnings("resource")
  public <R> R applyJedis(final ReadMode readMode, final int slot,
      final Function<Jedis, R> jedisConsumer, final int maxRetries) {

    Jedis askJedis = null;
    int retries = 0;
    try {

      // Optimistic first try
      Jedis jedis = null;
      try {
        jedis = connHandler.getConnectionFromSlot(readMode, slot);
        return jedisConsumer.apply(jedis);
      } catch (final JedisConnectionException jce) {

        if (maxRetries == 0) {
          throw jce;
        }
        retries = 1;
      } catch (final JedisMovedDataException jre) {

        if (jedis == null) {
          connHandler.renewSlotCache(readMode);
        } else {
          connHandler.renewSlotCache(readMode, jedis);
        }
      } catch (final JedisAskDataException jre) {

        askJedis = connHandler.getAskJedis(jre.getTargetNode());
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

          if (askJedis == null) {

            connection = retries > tryRandomAfter ? connHandler.getConnection(readMode)
                : connHandler.getConnectionFromSlot(readMode, slot);

            return jedisConsumer.apply(connection);
          }

          connection = askJedis;
          askJedis = null;
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
            connHandler.renewSlotCache(readMode);
          } else {
            connHandler.renewSlotCache(readMode, connection);
          }
          continue;
        } catch (final JedisAskDataException jre) {

          if (++redirections > maxRedirections) {
            throw new JedisClusterMaxRedirectionsException(jre);
          }

          askJedis = connHandler.getAskJedis(jre.getTargetNode());
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
      if (askJedis != null) {
        askJedis.close();
      }
    }
  }

  public void acceptAllMasters(final Consumer<Jedis> jedisConsumer) {

    acceptAllMasters(jedisConsumer, maxRetries);
  }

  public void acceptAllMasters(final Consumer<Jedis> jedisConsumer, final int maxRetries) {

    acceptAll(connHandler.getMasterPools(), jedisConsumer, maxRetries);
  }

  public void acceptAllSlaves(final Consumer<Jedis> jedisConsumer) {

    acceptAllSlaves(jedisConsumer, maxRetries);
  }

  public void acceptAllSlaves(final Consumer<Jedis> jedisConsumer, final int maxRetries) {

    acceptAll(connHandler.getSlavePools(), jedisConsumer, maxRetries);
  }

  public void acceptAll(final Consumer<Jedis> jedisConsumer) {

    acceptAll(jedisConsumer, maxRetries);
  }

  public void acceptAll(final Consumer<Jedis> jedisConsumer, final int maxRetries) {

    acceptAll(connHandler.getAllPools(), jedisConsumer, maxRetries);
  }

  private void acceptAll(final List<JedisPool> pools, final Consumer<Jedis> jedisConsumer,
      final int maxRetries) {
    JedisConnectionException failure = null;

    for (final JedisPool pool : pools) {

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

    connHandler.close();
  }

  public static final class Builder {

    private ReadMode readMode = ReadMode.MASTER;
    private Collection<HostAndPort> discoveryHostPorts;
    private int timeout = DEFAULT_TIMEOUT;
    private int soTimeout = DEFAULT_TIMEOUT;
    private int maxRedirections = DEFAULT_MAX_REDIRECTIONS;
    private int maxRetries = DEFAULT_MAX_RETRIES;
    private int tryRandomAfter = DEFAULT_TRY_RANDOM_AFTER;
    private GenericObjectPoolConfig poolConfig;
    private Function<HostAndPort, JedisPool> masterPoolFactory;
    private Function<HostAndPort, JedisPool> slavePoolFactory;
    private Function<JedisPool[], LoadBalancedPools> lbFactory;
    private boolean initReadOnly;

    private Builder() {}

    public JedisClusterExecutor create() {

      if (masterPoolFactory == null) {
        if (poolConfig == null) {
          poolConfig = new GenericObjectPoolConfig();
        }

        masterPoolFactory = node -> new JedisPool(poolConfig, node.getHost(), node.getPort(),
            timeout, soTimeout, null, 0, null);
      }

      if (slavePoolFactory == null) {
        slavePoolFactory = masterPoolFactory;
      }

      if (lbFactory == null) {
        lbFactory = slavePools -> new RoundRobinPools(slavePools);
      }

      return new JedisClusterExecutor(readMode, discoveryHostPorts, maxRedirections, maxRetries,
          tryRandomAfter, masterPoolFactory, slavePoolFactory, lbFactory, initReadOnly);
    }

    public ReadMode getReadMode() {
      return readMode;
    }

    public Builder withReadMode(final ReadMode readMode) {
      this.readMode = readMode;
      return this;
    }

    public Collection<HostAndPort> getDiscoveryHostPorts() {
      return discoveryHostPorts;
    }

    public Builder withDiscoveryHostPorts(final Collection<HostAndPort> discoveryHostPorts) {
      this.discoveryHostPorts = discoveryHostPorts;
      return this;
    }

    public int getTimeout() {
      return timeout;
    }

    public Builder withTimeout(final int timeout) {
      this.timeout = timeout;
      return this;
    }

    public int getSoTimeout() {
      return soTimeout;
    }

    public Builder withSoTimeout(final int soTimeout) {
      this.soTimeout = soTimeout;
      return this;
    }

    public int getMaxRedirections() {
      return maxRedirections;
    }

    public Builder withMaxRedirections(final int maxRedirections) {
      this.maxRedirections = maxRedirections;
      return this;
    }

    public int getMaxRetries() {
      return maxRetries;
    }

    public Builder withMaxRetries(final int maxRetries) {
      this.maxRetries = maxRetries;
      return this;
    }

    public int getTryRandomAfter() {
      return tryRandomAfter;
    }

    public Builder withTryRandomAfter(final int tryRandomAfter) {
      this.tryRandomAfter = tryRandomAfter;
      return this;
    }

    public GenericObjectPoolConfig getPoolConfig() {
      return poolConfig;
    }

    public Builder withPoolConfig(final GenericObjectPoolConfig poolConfig) {
      this.poolConfig = poolConfig;
      return this;
    }

    public Function<HostAndPort, JedisPool> getMasterPoolFactory() {
      return masterPoolFactory;
    }

    public Builder withMasterPoolFactory(final Function<HostAndPort, JedisPool> masterPoolFactory) {
      this.masterPoolFactory = masterPoolFactory;
      return this;
    }

    public Function<HostAndPort, JedisPool> getSlavePoolFactory() {
      return slavePoolFactory;
    }

    public Builder withSlavePoolFactory(final Function<HostAndPort, JedisPool> slavePoolFactory) {
      this.slavePoolFactory = slavePoolFactory;
      return this;
    }

    public Function<JedisPool[], LoadBalancedPools> getLbFactory() {
      return lbFactory;
    }

    public Builder withLbFactory(final Function<JedisPool[], LoadBalancedPools> lbFactory) {
      this.lbFactory = lbFactory;
      return this;
    }

    public boolean isInitReadOnly() {
      return initReadOnly;
    }

    public Builder withInitReadOnly(final boolean initReadOnly) {
      this.initReadOnly = initReadOnly;
      return this;
    }
  }
}
