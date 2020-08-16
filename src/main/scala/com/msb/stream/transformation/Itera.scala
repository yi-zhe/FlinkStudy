package com.msb.stream.transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object Itera {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val initStream = env.socketTextStream("node01", 8888)
    val stream = initStream.map(_.toLong)
    stream.iterate {
      iteration => {
        val iterationBody = iteration.map(x => {
          println("===" + x)
          if (x > 0) x - 1
          else x
        })
        (iterationBody.filter(_ > 0), iterationBody.filter(_ <= 0))
      }
    }.print()


    env.execute()
  }
}