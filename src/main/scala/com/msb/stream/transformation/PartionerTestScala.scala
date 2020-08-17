package com.msb.stream.transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object PartionerTestScala {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // shuffle rebalance 增大分区

    // shuffle 随机分给下游 网络开销比较大
//    val stream = env.generateSequence(1, 10).setParallelism(1)
//    println("parallelism=" + stream.getParallelism)
//    stream.shuffle.print()

    // rebalance 轮训分区元素 均匀地分发到下游分区
    val stream = env.generateSequence(1, 100).setParallelism(3)
    println("parallelism=" + stream.getParallelism)
    stream.rebalance.print()

    env.execute()
  }
}
