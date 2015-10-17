package com.fabahaba.jedipus.services;

import com.fabahaba.fava.service.curated.LeaderServiceConfig;
import com.fabahaba.fava.service.curated.ScheduledJobService;
import com.fabahaba.fava.service.curated.ScheduledJobState;
import com.fabahaba.jedipus.JedisExecutor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public abstract class RedisScheduledJobService extends ScheduledJobService {

  private final JedisExecutor redisExecutor;
  private final Gson gson;

  protected RedisScheduledJobService(final JedisExecutor redisExecutor,
      final LeaderServiceConfig serviceConfig, final Set<ScheduledJobState> scheduledJobPrototypes,
      final Gson gson) {
    super(serviceConfig, scheduledJobPrototypes);
    this.redisExecutor = redisExecutor;
    this.gson = gson;
  }

  @Override
  protected void persistPrototypes(final Collection<ScheduledJobState> scheduledJobPrototypes) {}

  @Override
  protected void persistJobState(final ScheduledJobState scheduledJob) {
    final String scheduledJobJson = gson.toJson(scheduledJob);

    redisExecutor.applyJedis(jedis -> jedis.set(scheduledJob.getName(), scheduledJobJson));
  }

  @Override
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
