package com.msb.stream.transformation

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * 自定义分区策略
 */
object CustomTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    val stream = env.generateSequence(1, 10).map((_, 1))
    stream.writeAsText("./data/stream1")
    // 0 根据第一个字段分区
    stream.partitionCustom(new CustomPartition(), 0)
        .writeAsText("./data/stream2")
        .setParallelism(4)
    env.execute()
  }

  class CustomPartition extends Partitioner[Long] {
    override def partition(key: Long, numPartitions: Int): Int = {
      key.toInt % numPartitions
    }
  }

}