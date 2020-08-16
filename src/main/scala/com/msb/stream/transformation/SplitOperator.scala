package com.msb.stream.transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object SplitOperator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 奇数一个流 偶数一个流
    val stream = env.generateSequence(1, 100)
    val splitStream = stream.split(d => {
      d % 2 match {
        case 0 => List("first")
        case 1 => List("second")
      }
    })

    // select 算子 根据标签获得流
    splitStream.select("first").print().setParallelism(1)
//    splitStream.select("second").print()

    env.execute()
  }
}