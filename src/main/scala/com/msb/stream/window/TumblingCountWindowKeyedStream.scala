package com.msb.stream.window

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * 当窗口中有10个元素, 统计这10个元素的word count
 */
object TumblingCountWindowKeyedStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.socketTextStream("node01", 8888)
    stream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(_._1)
      // 如果基于keyed stream, 看的是每一个key的数量是否大于10, 而不是总数大于10
      .countWindow(10)
//      .countWindow(10, 2)  滑动窗口
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(v1: (String, Int), v2: (String, Int)): (String, Int) = {
          (v1._1, v1._2 + v2._2)
        }
      })
      .print().setParallelism(1)

    env.execute()
  }
}