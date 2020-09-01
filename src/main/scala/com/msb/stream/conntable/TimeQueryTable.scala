package com.msb.stream.conntable

import java.util.{Timer, TimerTask}

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

import scala.collection.mutable
import scala.io.Source

/**
 * 场景二:对维度表更新的频率比较高, 并且要求实时性比较高
 * 通过Timer实现
 */
object TimeQueryTable {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.socketTextStream("node01", 8888)
    stream.map(new RichMapFunction[String, String] {

      private val map = new mutable.HashMap[String, String]

      override def open(parameters: Configuration): Unit = {
        println("init data ...")
        query()
        val timer = new Timer(true)
        timer.schedule(new TimerTask {
          override def run(): Unit = {
            query()
          }
        }, 1000, 9000)
      }

      def query(): Unit = {
        val source = Source.fromFile("./data/id2city", "UTF-8")
        val iterator = source.getLines()
        for (elem <- iterator) {
          val vs = elem.split(" ")
          map.put(vs(0), vs(1))
        }
      }

      override def map(key: String): String = {
        map.getOrElse(key, "city not found")
      }
    })
      .print()
    env.execute()
  }
}