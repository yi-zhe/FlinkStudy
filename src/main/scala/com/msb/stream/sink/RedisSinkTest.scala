package com.msb.stream.sink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

import scala.collection.mutable.ListBuffer

/**
 * 将WordCount的计算结果写入Redis
 * redis存数据需要是幂等的
 *
 * redis hset 支持幂等操作
 */
object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.socketTextStream("node01", 8888)

    val config = new FlinkJedisPoolConfig.Builder()
      .setHost("192.168.101.199")
      .setDatabase(1)
      .setPort(6379)
      .build()

    stream.flatMap(line => {
      val rest = new ListBuffer[(String, Int)]
      line.split(" ").foreach(word => rest += ((word, 1)))
      rest
    })
      .keyBy(_._1)
      .reduce((v1: (String, Int), v2: (String, Int)) => {
        (v1._1, v1._2 + v2._2)
      })
      .addSink(new RedisSink[(String, Int)](config, new RedisMapper[(String, Int)] {
        // 指定操作的Redis命令
        override def getCommandDescription: RedisCommandDescription = {
          new RedisCommandDescription(RedisCommand.HSET, "wc")
        }

        override def getKeyFromData(t: (String, Int)): String = {
          t._1
        }

        override def getValueFromData(t: (String, Int)): String = {
          t._2 + ""
        }
      }))

    env.execute()
  }
}