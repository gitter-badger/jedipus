#Jedipus [![Build Status](https://travis-ci.org/jamespedwards42/jedipus.svg?branch=master)](https://travis-ci.org/jamespedwards42/jedipus) [![JCenter](https://api.bintray.com/packages/jamespedwards42/libs/jedipus/images/download.svg) ](https://bintray.com/jamespedwards42/libs/jedipus/_latestVersion) [![License](http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat) ](http://www.apache.org/licenses/LICENSE-2.0)

>Jedipus provides a [JedisSentinelPoolExecutor](/jamespedwards42/jedipus/blob/master/src/main/java/com/fabahaba/jedipus/JedisSentinelPoolExecutor.java) which manages the state of a [JedisPool](https://github.com/xetorthio/jedis/wiki/Getting-started#basic-usage-example) and allows you to submit lambdas against [Jedis](https://github.com/xetorthio/jedis) clients.

###Usage
```java
final int database = 0;
final Collection<String> sentinelHostPorts =
    Lists.newArrayList("sentinelIp1:26379", "sentinelIp2:26379", "sentinelIp3:26379");
final JedisSentinelPoolExecutor jedisExecutor =
    ExtendedJedisPoolConfig.getDefaultConfig().buildExecutor("masterName", database,
        sentinelHostPorts, "password");

final int numRetries = 3;

// Simple Ping/Pong
final String pong = jedisExecutor.applyJedis(Jedis::ping, numRetries);
System.out.println(pong);

// Execute all commands transactionally, if any fail all will be rolled back.
final Response<String> keyValueResponse =
    jedisExecutor.applyPipelinedTransaction(jedisPipelined -> {
      jedisPipelined.set("key", "value");
      jedisPipelined.zadd("foo", 1, "barowitch");
      jedisPipelined.zadd("foo", 0, "barinsky");
      jedisPipelined.zadd("foo", 0, "barikoviev");
      return jedisPipelined.get("key");
    }, numRetries);

System.out.println(keyValueResponse.get()); // value
```

###Dependency Management
####Gradle
```groovy
repositories {
   jcenter()
}

dependencies {
   compile 'com.fabahaba:jedipus:1.0.8'
}
```
