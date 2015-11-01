package com.fabahaba.jedipus.services;

import com.fabahaba.fava.concurrent.ExecutorUtils;
import com.fabahaba.fava.logging.Loggable;
import com.fabahaba.fava.service.LeaderServiceI;
import com.fabahaba.fava.system.HostUtils;
import com.fabahaba.jedipus.JedisExecutor;
import com.google.common.util.concurrent.AbstractExecutionThreadService;

import redis.clients.jedis.Response;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public abstract class RedisLeaderService extends AbstractExecutionThreadService implements
    LeaderServiceI, Loggable {

  private final String serviceName;
  private final ScheduledExecutorService executorService;
  private final JedisExecutor jedisExecutor;
  private final long leadershipClaimDurationMillis;
  private final ReentrantLock leadershipLock;
  private final Condition isLeaderCondition;

  private volatile boolean isLeader = false;
  private long preventLeadershipBefore = 0;

  public RedisLeaderService(final String serviceName, final JedisExecutor jedisExecutor,
      final Duration leadershipClaimDuration) {

    this(serviceName, HostUtils.HOST_NAME, jedisExecutor, leadershipClaimDuration);
  }

  public RedisLeaderService(final String serviceName, final String leaderId,
      final JedisExecutor jedisExecutor, final Duration leadershipClaimDuration) {

    this.serviceName = serviceName;
    this.executorService = ExecutorUtils.newSingleThreadScheduledExecutor(serviceName);
    this.jedisExecutor = jedisExecutor;
    this.leadershipClaimDurationMillis = leadershipClaimDuration.toMillis();

    this.leadershipLock = new ReentrantLock();
    this.isLeaderCondition = leadershipLock.newCondition();

    executorService.scheduleAtFixedRate(new ManageLeaderState(serviceName, leaderId,
        (int) leadershipClaimDuration.getSeconds()), 0, leadershipClaimDuration.toNanos() / 2,
        TimeUnit.NANOSECONDS);
  }

  @Override
  public boolean hasLeadership() throws Exception {
    return isLeader;
  }

  @Override
  protected void run() throws Exception {

    for (;;) {
      if (hasLeadership()) {
        try {
          takeLeadership();
        } catch (final InterruptedException ie) {
          isLeader = false;
          onLeadershipRemoved();
          throw ie;
        } catch (final Throwable t) {
          leadershipLock.lock();
          try {
            final boolean wasLeader = isLeader;
            isLeader = false;
            final long claimLeadershipTimeout = leadershipClaimDurationMillis * 2;
            preventLeadershipBefore = System.currentTimeMillis() + claimLeadershipTimeout;
            if (wasLeader) {
              onLeadershipRemoved();
            }
          } catch (final Throwable t2) {
            error(t2);
          } finally {
            leadershipLock.unlock();
          }
        }
      } else {
        leadershipLock.lock();
        try {
          if (!isLeader) {
            isLeaderCondition.await();
          }
        } finally {
          leadershipLock.unlock();
        }
      }
    }
  }

  private static final byte[] NX = "NX".getBytes(StandardCharsets.UTF_8);
  private static final byte[] EX = "EX".getBytes(StandardCharsets.UTF_8);

  private class ManageLeaderState implements Runnable {

    private final byte[] serviceName;
    private final byte[] leaderId;
    private final int expireSeconds;

    public ManageLeaderState(final String serviceName, final String leaderId,
        final int expireSeconds) {
      this.serviceName = serviceName.getBytes(StandardCharsets.UTF_8);
      this.leaderId = leaderId.getBytes(StandardCharsets.UTF_8);
      this.expireSeconds = expireSeconds;
    }

    @Override
    public void run() {

      leadershipLock.lock();
      try {
        if (System.currentTimeMillis() < preventLeadershipBefore) {
          isLeader = false;
          return;
        }

        final Response<byte[]> currentLeaderResponse =
            jedisExecutor.applyPipelinedTransaction(pipeline -> {
              pipeline.set(serviceName, leaderId, NX, EX, expireSeconds);
              return pipeline.get(serviceName);
            }, 3);

        final byte[] currentLeaderBytes = currentLeaderResponse.get();
        final String currentLeader = new String(currentLeaderBytes, StandardCharsets.UTF_8);
        final boolean wasLeader = isLeader;
        isLeader = currentLeader.equals(HostUtils.HOST_NAME);

        if (wasLeader && isLeader) {
          jedisExecutor.acceptJedis(jedis -> jedis.expire(serviceName, expireSeconds), 2);
        }

        if (isLeader) {
          isLeaderCondition.signal();
        } else if (wasLeader) {
          onLeadershipRemoved();
        }

        return;
      } catch (final Throwable t) {
        isLeader = false;
        catching(t);
      } finally {
        leadershipLock.unlock();
      }
    }
  }

  @Override
  protected void startUp() throws Exception {}

  @Override
  protected void shutDown() throws Exception {
    executorService.shutdown();
  }

  @Override
  public String serviceName() {
    return serviceName;
  }
}
