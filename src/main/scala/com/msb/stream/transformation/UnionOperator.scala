package com.msb.stream.transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * 合并两个数据流(真合并)
 *
 * 条件: 数据流中的元素必须一致
 */
object UnionOperator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream1 = env.fromCollection(List(("a", 1), ("b", 2)))
    val stream2 = env.fromCollection(List(("c", 3), ("d", 4)))
    val unionStream = stream1.union(stream2)
    unionStream.print()
    env.execute()
  }
}
