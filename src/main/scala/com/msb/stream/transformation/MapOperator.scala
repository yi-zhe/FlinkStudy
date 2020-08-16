package com.msb.stream.transformation

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ListBuffer

object MapOperator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("node01", 8888)
    // 使用flatMap代替filter
    // flatMap : map + flat
    // 1
    //    stream.flatMap(x => {
    //      val rest = new ListBuffer[String]
    //      if (!x.contains("abc")) {
    //        rest += x
    //      }
    //      rest.iterator
    //    }).print()
    // 2. keyBy 分流算子
    stream
      .flatMap(_.split(" "))
      .map((_, 1))
      // 1
      //        .keyBy(0)
      // 2
      //      .keyBy(new KeySelector[(String, Int), String] {
      //        override def getKey(in: (String, Int)): String = {
      //          in._1
      //        }
      //      })
      // 3
      .keyBy(x => x._1)
      // 1
      //      .sum(1)
      // 2
      .reduce((v1, v2) => (v1._1, v1._2 + v2._2))
      .print()
    env.execute()
  }

}
