package com.fabahaba.jedipus;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.fabahaba.fava.func.Retryable;


public interface JedisExecutor extends Retryable {

    default String getMasterHostPort() {
        return applyJedis( jedis -> jedis.getClient().getHost() );
    }

    default void acceptPipeline(final Consumer<Pipeline> pipelineConsumer) {
        acceptJedis( jedis -> {
            final Pipeline pipeline = jedis.pipelined();
            pipelineConsumer.accept( pipeline );
            pipeline.sync();
        } );
    }

    default void acceptPipeline(final Consumer<Pipeline> pipelineConsumer, final int numRetries) {
        retryRun( () -> acceptPipeline( pipelineConsumer ), numRetries );
    }

    default void acceptPipelinedTransaction(final Consumer<Pipeline> pipelineConsumer) {
        acceptJedis( jedis -> {
            final Pipeline pipeline = jedis.pipelined();
            pipeline.multi();
            pipelineConsumer.accept( pipeline );
            pipeline.exec();
            pipeline.sync();
        } );
    }

    default void acceptPipelinedTransaction(final Consumer<Pipeline> pipelineConsumer, final int numRetries) {
        retryRun( () -> acceptPipelinedTransaction( pipelineConsumer ), numRetries );
    }

    public void acceptJedis(final Consumer<Jedis> jedisConsumer);

    default void acceptJedis(final Consumer<Jedis> jedisConsumer, final int numRetries) {
        retryRun( () -> acceptJedis( jedisConsumer ), numRetries );
    }

    default <T> T applyPipeline(final Function<Pipeline, T> pipelineConsumer) {
        return applyJedis( jedis -> {
            final Pipeline pipeline = jedis.pipelined();
            final T result = pipelineConsumer.apply( pipeline );
            pipeline.sync();
            return result;
        } );
    }

    default <T> T applyPipeline(final Function<Pipeline, T> pipelineConsumer, final int numRetries) {
        return retryCallWithThrow( () -> applyPipeline( pipelineConsumer ), numRetries );
    }

    default <T> T applyPipelinedTransaction(final Function<Pipeline, T> pipelineConsumer) {
        return applyJedis( jedis -> {
            final Pipeline pipeline = jedis.pipelined();
            pipeline.multi();
            final T result = pipelineConsumer.apply( pipeline );
            pipeline.exec();
            pipeline.sync();
            return result;
        } );
    }

    default <T> T applyPipelinedTransaction(final Function<Pipeline, T> pipelineConsumer, final int numRetries) {
        return retryCallWithThrow( () -> applyPipelinedTransaction( pipelineConsumer ), numRetries );
    }

    public <T> T applyJedis(final Function<Jedis, T> jedisFunc);

    default <T> T applyJedis(final Function<Jedis, T> jedisFunc, final int numRetries) {
        return retryCallWithThrow( () -> applyJedis( jedisFunc ), numRetries );
    }

    default <R> Optional<R> applyJedisOptionalWithThrow(final Function<Jedis, R> jedisFunction) {
        return Optional.ofNullable( applyJedis( jedisFunction ) );
    }

    default <R> Optional<R> applyJedisOptionalWithThrow(final Function<Jedis, R> jedisFunction, final int numRetries) {
        return Optional.ofNullable( applyJedis( jedisFunction, numRetries ) );
    }

    default <R> Optional<R> applyJedisOptional(final Function<Jedis, R> jedisFunction) {
        try {
            return applyJedisOptionalWithThrow( jedisFunction );
        } catch ( final JedisConnectionException rce ) {
            return Optional.empty();
        }
    }

    default <R> Optional<R> applyJedisOptional(final Function<Jedis, R> jedisFunction, final int numRetries) {
        try {
            return applyJedisOptionalWithThrow( jedisFunction, numRetries );
        } catch ( final JedisConnectionException rce ) {
            return Optional.empty();
        }
    }

    default Future<?> addTopicListener(final ExecutorService executorService, final JedisPubSub subscriber, final ReentrantLock subscribeLock, final String[] topics) {
        return executorService.submit( new RedisSubscribeJob( this, subscriber, subscribeLock, topics ) );
    }
}
