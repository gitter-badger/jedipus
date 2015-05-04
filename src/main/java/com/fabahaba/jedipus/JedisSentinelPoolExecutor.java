package com.fabahaba.jedipus;

import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Function;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Pool;

import com.fabahaba.fava.concurrent.AbstractMutable;
import com.fabahaba.fava.logging.Loggable;

public class JedisSentinelPoolExecutor extends AbstractMutable implements JedisExecutor, Loggable {

    private final String masterName;
    private final int db;
    private final Set<String> sentinelHostPorts;
    private final String password;
    private final ExtendedJedisPoolConfig poolConfig;

    private Pool<Jedis> sentinelPool;

    public JedisSentinelPoolExecutor(final String masterName, final int db, final Set<String> sentinelHostPorts, final String password,
            final ExtendedJedisPoolConfig poolConfig) {
        this.masterName = masterName;
        this.db = db;
        this.sentinelHostPorts = sentinelHostPorts;
        this.password = password;
        this.poolConfig = poolConfig;
    }

    @Override
    public void acceptJedis(final Consumer<Jedis> jedisConsumer) {
        executeReadOp( () -> {
            Jedis jedis = null;
            final Pool<Jedis> jedisPool = getPool();
            try {
                jedis = jedisPool.getResource();
                jedisConsumer.accept( jedis );
            } catch ( final JedisException je ) {
                jedis = handleConnectionException( je, jedisPool, jedis );
                throw je;
            } finally {
                returnJedis( jedisPool, jedis );
            }
        } );
    }

    @Override
    public <T> T applyJedis(final Function<Jedis, T> jedisFunc) {
        return executeReadOp( () -> {
            Jedis jedis = null;
            final Pool<Jedis> jedisPool = getPool();
            try {
                jedis = jedisPool.getResource();
                return jedisFunc.apply( jedis );
            } catch ( final JedisException je ) {
                jedis = handleConnectionException( je, jedisPool, jedis );
                throw je;
            } finally {
                returnJedis( jedisPool, jedis );
            }
        } );
    }

    private void returnJedis(final Pool<Jedis> jedisPool, final Jedis jedis) {
        try {
            if ( jedis != null ) {
                jedisPool.returnResource( jedis );
                numConsecutiveFailures.reset();
            }
        } catch ( final JedisException je ) {
            handleConnectionException( je, jedisPool, jedis );
        }
    }

    private Jedis handleConnectionException(final JedisException je, final Pool<Jedis> jedisPool, final Jedis brokenJedis) {
        reInitJedisSentinelPool( jedisPool );
        if ( brokenJedis != null )
            jedisPool.returnBrokenResource( brokenJedis );
        error( je );
        return null;
    }

    private synchronized Pool<Jedis> getPool() {
        return sentinelPool == null ? setSentinelPool( constructPool() ) : sentinelPool;
    }

    private Pool<Jedis> setSentinelPool(final Pool<Jedis> sentinelPool) {
        try {
            if ( this.sentinelPool != null )
                this.sentinelPool.close();
        } catch ( final Exception e ) {
            catching( e );
        }
        return this.sentinelPool = sentinelPool;
    }

    private Pool<Jedis> constructPool() {
        return new JedisSentinelPool( masterName, sentinelHostPorts, poolConfig, poolConfig.getConnectionTimeoutMillis(), password, db );
    }

    private final LongAdder numConsecutiveFailures = new LongAdder();

    private void reInitJedisSentinelPool(final Pool<Jedis> jedisPool) {
        launchWriteOp( () -> {
            if ( jedisPool.equals( getPool() ) ) {
                numConsecutiveFailures.increment();
                if ( numConsecutiveFailures.sum() > poolConfig.getMaxConsecutiveConnectionFailures() ) {
                    setSentinelPool( constructPool() );
                    numConsecutiveFailures.reset();
                }
            }
        } );
    }

    @Override
    public String toString() {
        return "JedisSentinelPoolExecutor [masterName=" + this.masterName + ", db=" + this.db + ", sentinelHostPorts=" + this.sentinelHostPorts
                + ", poolConfig=" + this.poolConfig + ", sentinelPool=" + this.sentinelPool + "]";
    }
}
