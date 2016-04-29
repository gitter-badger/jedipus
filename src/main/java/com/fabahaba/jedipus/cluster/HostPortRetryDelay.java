package com.fabahaba.jedipus.cluster;

import redis.clients.jedis.HostAndPort;

public interface HostPortRetryDelay {

  void markFailure(final HostAndPort hostPort);

  void markSuccess(final HostAndPort hostPort);
}
