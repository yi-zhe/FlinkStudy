package com.msb.stream.window

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object EventTimeWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    // 10000 hello spark
    val stream = env.socketTextStream("node01", 8888)
      .assignAscendingTimestamps(data => {
        val splits = data.split(" ")
        splits(0).toLong
      })

    stream.flatMap(_.split(" ").tail)
      .map((_, 1))
      .keyBy(_._1)
      // 区间左闭右开[start, end)
      .timeWindow(Time.seconds(3))
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(v1: (String, Int), v2: (String, Int)): (String, Int) = {
          (v1._1, v1._2 + v2._2)
        }
      }, new WindowFunction[(String, Int), (String, Int), String, TimeWindow] {
        override def apply(key: String, window: TimeWindow, input: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
          val start = window.getStart
          val end = window.getEnd
          println("window start:" + start + "--- end:" + end)
          for (elem <- input) {
            out.collect(elem)
          }
        }
      }).print()

    env.execute()
  }
}