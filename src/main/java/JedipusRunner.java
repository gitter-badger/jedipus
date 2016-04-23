import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.fabahaba.jedipus.cluster.JedisClusterExecutor;
import com.google.common.collect.ImmutableSet;

import redis.clients.jedis.HostAndPort;

public final class JedipusRunner {

  public static void main(final String[] args) throws IOException {

    try (JedisClusterExecutor cluster =
        new JedisClusterExecutor(ImmutableSet.of(new HostAndPort("192.168.64.2", 7000)))) {

      // cluster.acceptAllMasters(jedis -> {
      //
      // System.out.println("checking " + jedis.info());
      // });

      final byte[] key = "test".getBytes(StandardCharsets.UTF_8);
      cluster.acceptJedis(key, jedis -> {

        jedis.set(key, "yay".getBytes(StandardCharsets.UTF_8));
        System.out.println(new String(jedis.get(key), StandardCharsets.UTF_8));
        System.out.println(jedis.del(key));
      });
    }
  }
}
