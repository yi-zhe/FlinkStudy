package com.msb.stream.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object CustomSourceStandalone {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 发射的数据类型是[String]
    // 如果是基于SourceFunction接口实现自定义数据源, 只支持单并行度
    // Source: 1 is not a parallel source
    val stream = env.addSource(new SourceFunction[String] {
      var flag = true

      // 发射数据
      override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
        // run 读取任何地方数据 然后将数据发射出去
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
    })
    stream.print()
    env.execute()
  }
}
