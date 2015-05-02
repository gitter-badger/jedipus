package com.fabahaba.jedipus;

import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import redis.clients.jedis.JedisPubSub;

import com.fabahaba.fava.logging.Loggable;

public class RedisSubscribeJob implements Runnable, Loggable {

    private final JedisExecutor redisExecutor;
    private final JedisPubSub subscriber;
    private final String[] topics;
    private final ReentrantLock subscribeLock;

    RedisSubscribeJob(final JedisExecutor redisExecutor, final JedisPubSub subscriber, final ReentrantLock subscribeLock, final String[] topics) {
        this.redisExecutor = redisExecutor;
        this.subscriber = subscriber;
        this.topics = topics;
        this.subscribeLock = subscribeLock;
    }

    @Override
    public void run() {
        try {
            subscribeToChannels();
        } catch ( final Exception e ) {
            error( "Subscription to " + Arrays.toString( topics ) + " has failed." );
            throw e;
        }
    }

    private void subscribeToChannels() {
        redisExecutor.acceptJedis( jedis -> {
            try {
                subscribeLock.lock();
                jedis.subscribe( subscriber, topics );
            } finally {
                try {
                    if ( subscriber.isSubscribed() )
                        subscriber.unsubscribe( topics );
                } catch ( final Exception e ) {
                }
                try {
                    subscribeLock.unlock();
                } catch ( final Exception e ) {
                }
            }
        } );
    }
}
