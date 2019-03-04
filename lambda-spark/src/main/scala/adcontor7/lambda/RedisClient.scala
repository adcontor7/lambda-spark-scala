package adcontor7.lambda

import redis.clients.jedis.Jedis


class RedisClient(redisHost:String = "localhost:6379") {

  def getInstance: Jedis = {
    val Array(host, port) = redisHost.split(":")
    new Jedis(host, port.toInt)  //TODO-TD USE POOL
  }

}
