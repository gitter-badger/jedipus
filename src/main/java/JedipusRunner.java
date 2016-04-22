import java.io.IOException;

import com.fabahaba.jedipus.cluster.DirectJedisClusterExecutor;
import com.google.common.collect.ImmutableSet;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

public final class JedipusRunner {

  public static void main(final String[] args) throws IOException {

    try (JedisCluster cluster =
        new JedisCluster(ImmutableSet.of(new HostAndPort("104.197.120.247", 6379)))) {

      System.out.println(cluster.getClusterNodes());

      final DirectJedisClusterExecutor je = new DirectJedisClusterExecutor(cluster);
      je.acceptJedis(jedisCluster -> {

        jedisCluster.getClusterNodes().keySet().forEach(hostPort -> {

          System.out.println("checking " + hostPort);

        });
      });
    }
  }
}
