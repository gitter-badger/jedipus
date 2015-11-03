package com.fabahaba.jedipus.services;

import com.fabahaba.fava.service.curated.ScheduledJobState;
import com.fabahaba.fava.service.curated.ScheduledJobState.Result;
import com.fabahaba.jedipus.JedisExecutor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.gson.Gson;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public abstract class RedisScheduledJobService extends RedisLeaderService {

  private final JedisExecutor redisExecutor;
  private final Gson gson;
  private final ImmutableMap<String, ScheduledJobState> scheduledJobPrototypes;
  private final ListeningExecutorService executorService;

  protected RedisScheduledJobService(final String serviceName,
      final ListeningExecutorService executorService, final JedisExecutor redisExecutor,
      final Duration leadershipClaimDuration, final Set<ScheduledJobState> scheduledJobPrototypes,
      final Gson gson) {

    super(serviceName, redisExecutor, leadershipClaimDuration);

    this.redisExecutor = redisExecutor;
    this.gson = gson;
    this.executorService = executorService;
    this.scheduledJobPrototypes =
        scheduledJobPrototypes
            .stream()
            .collect(() -> ImmutableMap.<String, ScheduledJobState>builder(),
                (builder, jobPrototype) -> builder.put(jobPrototype.getName(), jobPrototype),
                (b1, b2) -> b1.putAll(b2.build())).build();
  }

  @Override
  public void takeLeadership() throws Exception {

    final Collection<ScheduledJobState> scheduledJobs = loadJobs(scheduledJobPrototypes);

    runReadyJobs(scheduledJobs);

    Thread.sleep(RedisScheduledJobService.getMillisUntilNextJobIsReady(scheduledJobs));
  }

  private void runReadyJobs(final Collection<ScheduledJobState> scheduledJobs) {

    scheduledJobs
        .stream()
        .filter(ScheduledJobState::isReady)
        .peek(
            scheduledJob -> persistJobState(scheduledJob.setStartEpochSeconds(Instant.now()
                .getEpochSecond())))
        .forEach(
            scheduledJob -> Futures.addCallback(executorService.submit(scheduledJob),
                new FutureCallback<Result>() {

                  @Override
                  public void onSuccess(final Result result) {

                    persistJobState(scheduledJob.setEndEpochSeconds(Instant.now().getEpochSecond())
                        .setResult(result));
                  }

                  @Override
                  public void onFailure(final Throwable t) {

                    persistJobState(scheduledJob.setEndEpochSeconds(Instant.now().getEpochSecond())
                        .setResult(Result.FAIL));
                  }
                }));
  }

  private static long
      getMillisUntilNextJobIsReady(final Collection<ScheduledJobState> scheduledJobs) {

    return scheduledJobs.stream().mapToLong(ScheduledJobState::getMillisUntilNextStart).min()
        .orElse(Long.MAX_VALUE);
  }

  protected void persistJobState(final ScheduledJobState scheduledJob) {

    final String scheduledJobJson = gson.toJson(scheduledJob);

    redisExecutor.applyJedis(jedis -> jedis.set(scheduledJob.getName(), scheduledJobJson));
  }

  protected Collection<ScheduledJobState> loadJobs(
      final ImmutableMap<String, ScheduledJobState> scheduledJobPrototypes) {

    if (scheduledJobPrototypes.isEmpty())
      return ImmutableList.of();

    final String[] jobNames = scheduledJobPrototypes.keySet().stream().toArray(String[]::new);
    final List<String> jobStates = redisExecutor.applyJedis(jedis -> jedis.mget(jobNames));

    final List<ScheduledJobState> loadedJobStates = new ArrayList<>(jobNames.length);

    for (int i = 0; i < jobNames.length; i++) {

      final String jobStateJson = jobStates.get(i);

      final ScheduledJobState prototype = scheduledJobPrototypes.get(jobNames[i]);

      if (jobStateJson == null) {
        loadedJobStates.add(prototype);
        continue;
      }

      final ScheduledJobState loadedJobState =
          gson.fromJson(jobStateJson, prototype.getClass())
              .setDelaySeconds(prototype.getDelaySeconds()).setDelayType(prototype.getDelayType())
              .setMaxDelaySeconds(prototype.getMaxDelaySeconds());

      loadedJobStates.add(loadedJobState);
    }

    return loadedJobStates;
  }
}
