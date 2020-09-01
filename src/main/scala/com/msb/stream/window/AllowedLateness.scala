package com.msb.stream.window

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AllowedLateness {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env.socketTextStream("node01", 8888)
    //定义了测输出流的标签
    val lateTag = new OutputTag[(Long, String)]("late")
    val value = stream.map(x => {
      val strings = x.split(" ")
      (strings(0).toLong, strings(1))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(Long, String)]
    (Time.seconds(2)) {
      override def extractTimestamp(element: (Long, String)): Long = element._1
    }).timeWindowAll(Time.seconds(5))
      //窗口触发之后的3s内，如果又出现了这个窗口的数据，这个窗口会重复计算  相当于说窗口保留3s钟
      .allowedLateness(Time.seconds(3))
      //如果数据迟到时间 超过5s 那么输出到侧输出流钟
      .sideOutputLateData(lateTag)
      //处理的是主流的数据，不会处理迟到非常严重的数据（已经输出到侧输出流）
      .process(new ProcessAllWindowFunction[(Long, String), (Long, String), TimeWindow] {
        override def process(context: Context, elements: Iterable[(Long, String)], out: Collector[(Long, String)]): Unit = {
          println(context.window.getStart + "---" + context.window.getEnd)
          for (elem <- elements) {
            out.collect(elem)
          }
        }
      })
    //打印的时候，会加上main这个前缀
    value.print("main")
    //获取侧输出流          //打印的时候，会加上late这个前缀
    value.getSideOutput(lateTag).print("late")
    env.execute()
  }
}

// 1. AllowedLateness(3s) + watermark(2s) = watermark(5s)是错误的
// 不够实时
// 但使用AllowedLateness数据冗余比较大, 会影响Flink的吞吐量
// 如果频繁的触发AllowedLateness机制, 那么有必要将watermark设置大一些
// 2. AllowedLateness(3s) + watermark(2s) = watermark(5s) + slide(2s) 是错误的
// 不够实时