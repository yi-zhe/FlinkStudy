package com.msb.stream.transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * 上游分区的所有数据发送到下游的第一个分区中
 */
object GlobalTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.generateSequence(1, 10).setParallelism(2)
    stream.writeAsText("./data/stream1").setParallelism(2)
    stream.global.writeAsText("./data/stream2").setParallelism(2)
    env.execute()
  }
}