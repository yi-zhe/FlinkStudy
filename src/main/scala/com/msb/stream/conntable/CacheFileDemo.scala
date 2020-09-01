package com.msb.stream.conntable

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.FileUtils

import scala.collection.mutable

/**
 * 维度表信息基本不变或变更频率很低
 */
object CacheFileDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 只会执行一次, 如果文件有更新, 不会刷到flink中
    // 每个TaskManager下都会有一份
    env.registerCachedFile("./data/id2city", "id2city")
    val socketStream = env.socketTextStream("node01", 8888)
    val stream = socketStream.map(_.toInt)
    stream.map(new RichMapFunction[Int, String] {

      private val id2CityMap = new mutable.HashMap[Int, String]

      override def open(parameters: Configuration): Unit = {
        val file = getRuntimeContext.getDistributedCache.getFile("id2city")
        val str = FileUtils.readFileUtf8(file)
        val strings = str.split("\r\n")
        for (str <- strings) {
          val splits = str.split(" ")
          val id = splits(0).toInt
          val city = splits(1)
          id2CityMap.put(id, city)
        }
      }

      override def map(key: Int): String = {
        id2CityMap.getOrElse(key, "not found city")
      }
    }).print()
    env.execute()
  }
}