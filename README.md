#Jedipus [![Build Status](https://travis-ci.org/jamespedwards42/Jedipus.svg?branch=master)](https://travis-ci.org/jamespedwards42/Jedipus) [![JCenter](https://api.bintray.com/packages/jamespedwards42/libs/jedipus/images/download.svg) ](https://bintray.com/jamespedwards42/libs/jedipus/_latestVersion)

>Jedipus provides a [JedisSentinelPoolExecutor](/jamespedwards42/jedipus/blob/master/src/main/java/com/fabahaba/jedipus/JedisSentinelPoolExecutor.java) which manages the state of a [JedisPool](https://github.com/xetorthio/jedis/wiki/Getting-started#basic-usage-example) and allows you to submit functions against [Jedis](https://github.com/xetorthio/jedis) clients.

###Usage
```java
final int database = 0;
final Set<String> sentinelHostPorts = Sets.newHashSet( "sentinelIp1:26379", "sentinelIp2:26379", "sentinelIp3:26379" );
final ExtendedJedisPoolConfig extendedPoolConifg = new ExtendedJedisPoolConfig().withMaxTotal( 100 ).withConnectionTimeoutMillis( 4000 );
final JedisSentinelPoolExecutor jedisExecutor = new JedisSentinelPoolExecutor( "masterName", database, sentinelHostPorts, "password", extendedPoolConifg );

final int numRetries = 3;
// Simple Ping/Pong
final String pong = jedisExecutor.applyJedis( Jedis::ping, numRetries );
System.out.println( pong );

// Execute all commands transactionally, if any fail all will be rolled back.
final Response<String> keyValueResponse = jedisExecutor.applyPipelinedTransaction(
  jedisPipelined -> {
    jedisPipelined.set( "key", "value" );
    jedisPipelined.zadd( "foo", 1, "barowitch" );
    jedisPipelined.zadd( "foo", 0, "barinsky" );
    jedisPipelined.zadd( "foo", 0, "barikoviev" );
    return jedisPipelined.get( "key" );
  }, numRetries );

System.out.println( keyValueResponse.get() ); // value
```

###Dependency Management
####Gradle
```groovy
repositories {
   jcenter()
}

dependencies {
   compile 'com.fabahaba:jedipus:0.1.1'
}
```
