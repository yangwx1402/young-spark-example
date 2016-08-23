package com.young.scala.spark.example.redis

import redis.clients.jedis.Jedis

/**
 * Created by Administrator on 2016/8/10.
 */
object RedisTools {

  val jedis = new Jedis("115.29.47.216",6379)

  def put(key:String,value:String): Unit ={
    jedis.set(key,value)
  }

  def get(key:String):String={
    jedis.get(key)
  }

  def main(args: Array[String]) {
    RedisTools.put("yangyong","niubi")
    println(RedisTools.get("yangyong"))
  }
}
