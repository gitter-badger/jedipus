#Jedipus [![Build Status](https://travis-ci.org/jamespedwards42/jedipus.svg?branch=master)](https://travis-ci.org/jamespedwards42/jedipus) [![JCenter](https://api.bintray.com/packages/jamespedwards42/libs/jedipus/images/download.svg) ](https://bintray.com/jamespedwards42/libs/jedipus/_latestVersion) [![License](http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat) ](http://www.apache.org/licenses/LICENSE-2.0)

>Jedipus is a Redis Cluster Java client that manages JedisPool's against masters of the cluster.

**Features:**
* Execute Jedis Consumer and Function Java 8 lambas against a Redis Cluster.
* Use known slot ints for O(1) direct primitive array access to a corresponding JedisPool.
* Minimal locking is applied using a StampedLock optimistic read when retrieiving a JedisPool.  Locking is required to handle concurrent Redis hash slot remapping events.
* Re-uses the work already done on Jedis clients to support pipelining and transactions.  Remember that all keys must share the same hash slot, instead of validating this, Jedipus trusts the user in order to avoid unnecessary overhead.
* Minimal dependency tree (Jedipus -> Jedis -> org.apache.commons:commons-pool2).
* Utilities to manage and execute Lua scripts.
* Optional user supplied HostAndPort -> JedisPool factories.  Useful for client side ip/port mapping or using different pool sizes per node.

###Basic Usage Example
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
} catch (final IOException e) {
  throw new UncheckedIOException(e);
}
```

###Redis Lock Lua Example

```java
public final class RedisLock {

  private RedisLock() {}

  private static final LuaScriptData TRY_ACQUIRE_LOCK =
      LuaScriptData.fromResourcePath("/TRY_ACQUIRE_LOCK.lua");

  private static final LuaScriptData TRY_RELEASE_LOCK =
      LuaScriptData.fromResourcePath("/TRY_RELEASE_LOCK.lua");

  @SuppressWarnings("unchecked")
  public static void main(final String[] args) {

    final Collection<HostAndPort> discoveryNodes =
        Collections.singleton(new HostAndPort("127.0.0.1", 7000));

    final int numRetries = 1;
    final int numKeys = 1;

    try (final JedisClusterExecutor jce = new JedisClusterExecutor(discoveryNodes)) {

      LuaScript.loadMissingScripts(jce, TRY_ACQUIRE_LOCK, TRY_RELEASE_LOCK);

      final byte[] lockName = RESP.toBytes("mylock");
      final byte[] ownerId = RESP.toBytes("myOwnerId");
      final byte[] pexpire = RESP.toBytes(3000);

      final List<Object> lockOwners = (List<Object>) TRY_ACQUIRE_LOCK.eval(jce, numRetries, numKeys,
          lockName, ownerId, pexpire);

      // final byte[] previousOwner = (byte[]) lockOwners.get(0);
      final byte[] currentOwner = (byte[]) lockOwners.get(1);
      final long pttl = (long) lockOwners.get(2);

      // 'myOwnerId' has lock 'mylock' for 3000ms.
      System.out.format("'%s' has lock '%s' for %dms.%n", RESP.toString(currentOwner),
          RESP.toString(lockName), pttl);

      final long released =
          (long) TRY_RELEASE_LOCK.eval(jce, numRetries, numKeys, lockName, ownerId);

      if (released == 1) {
        // Lock was released by 'myOwnerId'.
        System.out.format("Lock was released by '%s'.%n", RESP.toString(ownerId));
      } else {
        System.out.format("Lock was no longer owned by '%s'.%n", RESP.toString(ownerId));
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
```

**src/main/resoures/TRY_ACQUIRE_LOCK.lua**
```lua
-- Returns the previous owner, the current owner and the pttl for the lock.
-- Returns either {null, lockOwner, pexpire}, {owner, owner, pexpire} or {owner, owner, pttl}.
-- The previous owner is null if 'lockOwner' newly acquired the lock. Otherwise, the previous
--   owner will be same value as the current owner. If the current owner is equal to the supplied
--   'lockOwner' argument then the ownership claim will remain active for 'pexpire' milliseconds.

local lockName = KEYS[1];
local lockOwner = ARGV[1];

local owner = redis.call('get', lockName);

if not owner then

   local pexpire = tonumber(ARGV[2]);

   redis.call('psetex', lockName, pexpire, lockOwner);
   return {owner, lockOwner, pexpire};
end

if owner == lockOwner then

   local pexpire = tonumber(ARGV[2]);

   if redis.call('pexpire', lockName, pexpire) == 0 then
      redis.call('psetex', lockName, pexpire, lockOwner)
   end

   return {owner, owner, pexpire};
end

return {owner, owner, redis.call('pttl', lockName)};
```

**src/main/resoures/TRY_RELEASE_LOCK.lua**
```lua
local lockName = KEYS[1];
local lockOwner = ARGV[1];

if redis.call('get', lockName) == lockOwner then
   return redis.call('del', lockName);
end

return -1;
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
