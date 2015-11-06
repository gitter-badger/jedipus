package com.fabahaba.jedipus;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.InvalidURIException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.JedisURIHelper;

import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

public class PublicJedisFactory implements PooledObjectFactory<Jedis> {

  private final AtomicReference<HostAndPort> hostAndPort = new AtomicReference<>();
  private final int connectionTimeout;
  private final int soTimeout;
  private final String password;
  private final int database;
  private final String clientName;

  public PublicJedisFactory(final String host, final int port, final int connectionTimeout,
      final int soTimeout, final String password, final int database, final String clientName) {

    this.hostAndPort.set(new HostAndPort(host, port));
    this.connectionTimeout = connectionTimeout;
    this.soTimeout = soTimeout;
    this.password = password;
    this.database = database;
    this.clientName = clientName;
  }

  public PublicJedisFactory(final URI uri, final int connectionTimeout, final int soTimeout,
      final String clientName) {

    if (!JedisURIHelper.isValid(uri))
      throw new InvalidURIException(String.format(
          "Cannot open Redis connection due to invalid URI. %s", uri.toString()));

    this.hostAndPort.set(new HostAndPort(uri.getHost(), uri.getPort()));
    this.connectionTimeout = connectionTimeout;
    this.soTimeout = soTimeout;
    this.password = JedisURIHelper.getPassword(uri);
    this.database = JedisURIHelper.getDBIndex(uri);
    this.clientName = clientName;
  }

  public void setHostAndPort(final HostAndPort hostAndPort) {
    this.hostAndPort.set(hostAndPort);
  }

  @Override
  public void activateObject(final PooledObject<Jedis> pooledJedis) throws Exception {

    final BinaryJedis jedis = pooledJedis.getObject();
    if (jedis.getDB() != database) {
      jedis.select(database);
    }
  }

  @Override
  public void destroyObject(final PooledObject<Jedis> pooledJedis) throws Exception {

    final BinaryJedis jedis = pooledJedis.getObject();
    if (jedis.isConnected()) {
      try {
        try {
          jedis.quit();
        } finally {
          jedis.disconnect();
        }
      } catch (final Exception e) {

      }
    }
  }

  @Override
  public PooledObject<Jedis> makeObject() throws Exception {

    final HostAndPort hostAndPort = this.hostAndPort.get();
    final Jedis jedis =
        new Jedis(hostAndPort.getHost(), hostAndPort.getPort(), connectionTimeout, soTimeout);

    jedis.connect();

    if (null != this.password) {
      jedis.auth(this.password);
    }

    if (database != 0) {
      jedis.select(database);
    }

    if (clientName != null) {
      jedis.clientSetname(clientName);
    }

    return new DefaultPooledObject<Jedis>(jedis);
  }

  @Override
  public void passivateObject(final PooledObject<Jedis> pooledJedis) throws Exception {}

  @Override
  public boolean validateObject(final PooledObject<Jedis> pooledJedis) {

    final BinaryJedis jedis = pooledJedis.getObject();
    final HostAndPort hostAndPort = this.hostAndPort.get();

    final String connectionHost = jedis.getClient().getHost();
    final int connectionPort = jedis.getClient().getPort();

    try {
      return hostAndPort.getHost().equals(connectionHost)
          && hostAndPort.getPort() == connectionPort && jedis.isConnected()
          && jedis.ping().equals("PONG");
    } catch (JedisDataException | JedisConnectionException je) {
      return false;
    }
  }
}
