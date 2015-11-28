package com.fabahaba.jedipus;

import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

public class JedisPoolFactory {

  private static final Logger LOG = LoggerFactory.getLogger(JedisPoolFactory.class);

  public static ObjectPool<Jedis> createMasterPool(final String masterName,
      final Set<String> sentinels, final GenericObjectPoolConfig poolConfig,
      final int connectionTimeout, final int soTimeout, final String password, final int database,
      final String clientName, final Function<HostAndPort, HostAndPort> hostPortMapper) {

    final HostAndPort masterHostPort =
        JedisPoolFactory.getMasterFromSentinels(sentinels, masterName, hostPortMapper);

    final PooledObjectFactory<Jedis> jedisFactory = new PublicJedisFactory(masterHostPort.getHost(),
        masterHostPort.getPort(), connectionTimeout, soTimeout, password, database, clientName);

    return new FinalMasterJedisPool<>(poolConfig, jedisFactory);
  }

  public static HostAndPort getMasterFromSentinels(final Set<String> sentinels,
      final String masterName, final Function<HostAndPort, HostAndPort> hostPortMapper) {

    HostAndPort masterHostPort = null;
    boolean sentinelAvailable = false;

    LOG.info("Trying to find master from available Sentinels...");

    for (final String sentinel : sentinels) {

      final HostAndPort sentinelHostPort =
          JedisPoolFactory.toHostAndPort(Arrays.asList(sentinel.split(":")));

      LOG.trace("Connecting to Sentinel {}.", sentinelHostPort);

      try (Jedis jedis = new Jedis(sentinelHostPort.getHost(), sentinelHostPort.getPort())) {

        final List<String> masterAddr = jedis.sentinelGetMasterAddrByName(masterName);

        sentinelAvailable = true;

        if (masterAddr == null || masterAddr.size() != 2) {
          LOG.warn("Can not get master addr, master name: {}. Sentinel: {}.", masterName,
              sentinelHostPort);
          continue;
        }

        masterHostPort =
            Objects.requireNonNull(hostPortMapper.apply(JedisPoolFactory.toHostAndPort(masterAddr)),
                "Unable to map master " + masterAddr);

        LOG.trace("Retreived master {} from sentinel {}.", masterAddr, sentinelHostPort);

        return masterHostPort;
      } catch (final JedisConnectionException e) {
        LOG.error("Cannot connect to sentinel running @ {}. Trying next one.", sentinelHostPort);
      }
    }

    if (sentinelAvailable)
      throw new JedisException(
          "Can connect to sentinel, but " + masterName + " seems to be not monitored...");

    throw new JedisConnectionException(
        "All sentinels down, cannot determine where is " + masterName + " master is running...");
  }

  public static HostAndPort toHostAndPort(final List<String> hostPortStringParts) {

    final String host = hostPortStringParts.get(0);
    final int port = Integer.parseInt(hostPortStringParts.get(1));

    return new HostAndPort(host, port);
  }
}
