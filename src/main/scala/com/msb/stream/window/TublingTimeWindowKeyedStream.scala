package com.msb.stream.window

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// 1. 每隔10秒计算最近N秒数据的wordcount
// 2. 将计算结果写入MySQL
object TublingTimeWindowKeyedStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val initStream: DataStream[String] = env.socketTextStream("node01", 8888)
    val wordStream = initStream.flatMap(_.split(" "))
    val pairStream = wordStream.map((_, 1))
    // 分好流的无界流
    val keyByStream = pairStream.keyBy(_._1)
    keyByStream
      // 一个参数是滚动窗口, 两个参数是滑动窗口
      .timeWindow(Time.seconds(10))
      //      .reduce(new ReduceFunction[(String, Int)] {
      //        override def reduce(v1: (String, Int), v2: (String, Int)): (String, Int) = {
      //          (v1._1, v1._2 + v2._2)
      //        }
      //      })
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(v1: (String, Int), v2: (String, Int)): (String, Int) = {
          (v1._1, v1._2 + v2._2)
        }
      }, new WindowFunction[(String, Int), (String, Int), String, TimeWindow] {
        override def apply(key: String, window: TimeWindow, input: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
          // 后续处理每个窗口调用一次
          // TODO
        }
      })
      .print()


    env.execute()
  }
}