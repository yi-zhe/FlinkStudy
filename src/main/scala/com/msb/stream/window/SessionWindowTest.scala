package com.msb.stream.window

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object SessionWindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("node01", 8888)
    // Session生命周期10s 如果窗口中连续10s都没有数据进入 窗口就会滑动(触发计算)
    stream.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
      .process(new ProcessAllWindowFunction[String, String, TimeWindow] {
        override def process(context: Context, elements: Iterable[String], out: Collector[String]): Unit = {
          for (elem <- elements) {
            out.collect(elem)
          }
        }
      })
      .print()

    env.execute()
  }
}