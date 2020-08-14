package com.msb.stream.source

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import scala.util.Random

// 多并行度的数据源
object ParallelCustomSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new ParallelSourceFunction[String] {
      var flag = true

      override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
        val random = new Random()
        while (flag) {
          sourceContext.collect("hello " + random.nextInt(1000))
          Thread.sleep(100)
        }
      }

      // 停止
      override def cancel(): Unit = {
        flag = false
      }
    }).setParallelism(2)
    stream.print().setParallelism(2)
    env.execute()
  }
}
