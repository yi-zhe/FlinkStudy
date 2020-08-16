package com.msb.stream.transformation

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import redis.clients.jedis.Jedis

object WC2Redis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("node01", 8888)
    val restStream = stream.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)
    restStream.map(new RichMapFunction[(String, Int), String] {
      var jedis: Jedis = _

      override def map(value: (String, Int)): String = {
        jedis.set(value._1, value._2 + "")
        value._1
      }

      override def open(parameters: Configuration): Unit = {
        val name = getRuntimeContext.getTaskName
        val subtaskName = getRuntimeContext.getTaskNameWithSubtasks
        println("name:" + name + " subtask name:" + subtaskName)
        jedis = new Jedis("node01", 6379)
        jedis.select(3)
      }

      override def close(): Unit = {
        jedis.close()
      }
    })

    env.execute()
  }
}