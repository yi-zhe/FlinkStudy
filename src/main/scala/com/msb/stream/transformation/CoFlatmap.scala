package com.msb.stream.transformation

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
 * 现在有一个配置文件存储车牌号与车主的真实姓名
 * 通过数据流中的车牌号实时匹配出对应的车主姓名
 * 注意: 配置文件可能随时改变
 * 应该使用readFile, 因为readTextFile只读一次
 */
object CoFlatmap {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val filePath = "data/carId2Name"
    val carIdNameStream = env.readFile(new TextInputFormat(
      new Path(filePath)), filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000)

    val dataStream = env.socketTextStream("node01", 8888)

    dataStream
      .connect(carIdNameStream)
      .map(new CoMapFunction[String, String, String] {
        private val hashMap = new mutable.HashMap[String, String]()

        override def map1(v1: String): String = {
          hashMap.getOrElse(v1, "not found name")
        }

        override def map2(v2: String): String = {
          val splits = v2.split(" ")
          hashMap.put(splits(0), splits(1))
          v2 + "加载完毕..."
        }
      })
      .print()

    env.execute()
  }
}