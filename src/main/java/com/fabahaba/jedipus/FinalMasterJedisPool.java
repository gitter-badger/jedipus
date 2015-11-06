package com.fabahaba.jedipus;

import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.Closeable;

public class FinalMasterJedisPool<J extends Jedis> implements Closeable, ObjectPool<J> {

  private final GenericObjectPool<J> internalPool;

  protected FinalMasterJedisPool(final GenericObjectPoolConfig poolConfig,
      final PooledObjectFactory<J> factory) {

    this.internalPool = new GenericObjectPool<J>(factory, poolConfig);
  }

  @Override
  public int getNumActive() {

    return internalPool.isClosed() ? -1 : internalPool.getNumActive();
  }

  @Override
  public int getNumIdle() {

    return internalPool.isClosed() ? -1 : internalPool.getNumIdle();
  }

  @Override
  public J borrowObject() throws Exception {

    try {
      return internalPool.borrowObject();
    } catch (final Exception e) {
      throw new JedisConnectionException("Could not get a resource from the pool.", e);
    }
  }

  @Override
  public void returnObject(final J obj) throws Exception {

    if (obj == null)
      return;

    internalPool.returnObject(obj);
  }

  @Override
  public void invalidateObject(final J obj) throws Exception {

    if (obj == null)
      return;

    internalPool.invalidateObject(obj);
  }

  @Override
  public void addObject() throws Exception {

    internalPool.addObject();
  }

  @Override
  public void close() {

    internalPool.close();
  }

  @Override
  public void clear() throws Exception {

    internalPool.clear();
  }
}
