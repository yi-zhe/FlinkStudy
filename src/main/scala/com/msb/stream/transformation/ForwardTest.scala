package com.msb.stream.transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * 上游分区与下游分区一一对应的分发,
 * 上下游分区数必须一致
 */
object ForwardTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.generateSequence(1, 10).setParallelism(2)
    stream.writeAsText("./data/stream1").setParallelism(2)
    // 不允许改变并行度
//    stream.forward.writeAsText("./data/stream2").setParallelism(4)
    stream.forward.writeAsText("./data/stream2").setParallelism(2)
    env.execute()
  }
}