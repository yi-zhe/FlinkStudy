package com.msb.stream.window

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object TumblingCountWindowNoKeyedStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("node01", 8888)
    stream.flatMap(_.split(" "))
      .map((_, 1))
      // 因为不是基于keyed stream之上的窗口, 所以只需要看窗口中的元素数就可以, 不需要看相同元素的个数
      .countWindowAll(5)
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(v1: (String, Int), v2: (String, Int)): (String, Int) = {
          (v1._1 + "," + v2._1, v1._2 + v2._2)
        }
      })
      .print()

    env.execute()
  }
}