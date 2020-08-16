package com.msb.stream.transformation

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * 假合并
 */
object ConnectOperator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val ds1 = env.socketTextStream("node01", 8888)
    val ds2 = env.socketTextStream("node01", 9999)
    val wcStream1 = ds1.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)
    val wcStream2 = ds2.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)
    val realStream = wcStream2.connect(wcStream1)
    realStream.map(new CoMapFunction[(String, Int), (String, Int), (String, Int)] {
      // 处理Connect前面的元素
      override def map1(value: (String, Int)): (String, Int) = {
        println("wcStream2:" + value)
        value
      }

      // 处理Connect后面的元素即使wcStream1
      override def map2(value: (String, Int)): (String, Int) = {
        println("wcStream1:" + value)
        value
      }
    })

    env.execute()
  }
}