package com.msb.stream.transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * 下游的分区会接收到上游所有分区的数据
 */
object BroadcastTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.generateSequence(1, 10).setParallelism(2)
    stream.writeAsText("./data/stream1").setParallelism(2)
    stream.broadcast.writeAsText("./data/stream2").setParallelism(4)
    env.execute()
  }
}