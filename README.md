#Jedipus [![Build Status](https://travis-ci.org/jamespedwards42/jedipus.svg?branch=master)](https://travis-ci.org/jamespedwards42/jedipus) [![JCenter](https://api.bintray.com/packages/jamespedwards42/libs/jedipus/images/download.svg) ](https://bintray.com/jamespedwards42/libs/jedipus/_latestVersion) [![License](http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat) ](http://www.apache.org/licenses/LICENSE-2.0)

>Jedipus is a Redis Cluster Java client that manages JedisPool's against masters of the cluster.

Features:
* Execute Jedis Consumer and Function Java 8 lambas against a Redis Cluster.
* Use known slot ints for O(1) direct primitive array access to a corresponding JedisPool.
* Minimal locking is applied using a StampedLock optimistic read when retrieiving a JedisPool.  Locking is required to handle conccurent Redis hash slot remapping events.
* Re-uses the work already done on Jedis clients to support pipelining and transactions.  Remember that all keys must share the same hash slot, instead of validating this, Jedipus trusts the user in order to avoid unnecessary overhead.
* Minimal dependency tree (Jedipus -> Jedis -> org.apache.commons:commons-pool2).
* Utilities to manage and execute Lua scripts.

###Usage
```java
final Collection<HostAndPort> discoveryNodes =
    Collections.singleton(new HostAndPort("127.0.0.1", 7000));

try (final JedisClusterExecutor jce = new JedisClusterExecutor(discoveryNodes)) {

  // Ping-Pong all masters.
  jce.acceptAllMasters(jedis -> System.out.format("%s:%d %s%n", jedis.getClient().getHost(),
      jedis.getClient().getPort(), jedis.ping()));

  // Hash tagged pipelined transaction.
  final String hashTag = RCUtils.createNameSpacedHashTag("HT");
  final int slot = JedisClusterCRC16.getSlot(hashTag);

  final List<Response<?>> results = new ArrayList<>(2);
  final String hashTaggedKey = hashTag + "key";
  final String fooKey = hashTag + "foo";

  jce.acceptPipelinedTransaction(slot, pipeline -> {

    pipeline.set(hashTaggedKey, "value");
    pipeline.zadd(fooKey, 1, "barowitch");
    pipeline.zadd(fooKey, 0, "barinsky");
    pipeline.zadd(fooKey, 0, "barikoviev");

    results.add(pipeline.get(hashTaggedKey));
    results.add(pipeline.zrangeWithScores(fooKey, 0, -1));
  });

  // '{HT}:key': value
  System.out.format("%n'%s': %s%n", hashTaggedKey, results.get(0).get());

  @SuppressWarnings("unchecked")
  final Set<Tuple> zrangeResult = (Set<Tuple>) results.get(1).get();
  final String values = zrangeResult.stream()
      .map(tuple -> String.format("%s (%s)", tuple.getElement(), tuple.getScore()))
      .collect(Collectors.joining(", "));

  // '{HT}:foo': [barikoviev (0.0), barinsky (0.0), barowitch (1.0)]
  System.out.format("%n'%s': [%s]%n", fooKey, values);

  // cleanup
  jce.acceptJedis(slot, jedis -> jedis.del(hashTaggedKey, fooKey));
}
```

###Dependency Management
####Gradle
```groovy
repositories {
   jcenter()
}

dependencies {
   compile 'redis.clients:jedis:+'
   compile 'com.fabahaba:jedipus:+'
}
```
