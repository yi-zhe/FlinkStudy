package com.msb.stream.state

import org.apache.flink.api.common.functions.{ReduceFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * 统计所有车的速度总和
 */
object MapStateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("node01", 8888)
    stream.map(data => {
      val splits = data.split(" ")
      (splits(0), splits(1).toLong)
    }).keyBy(_._1)
      .map(new RichMapFunction[(String, Long), (String, Long)] {

        private var speedCount: ReducingState[Long] = _

        override def open(parameters: Configuration): Unit = {
          val desc = new ReducingStateDescriptor[Long]("reduce", new ReduceFunction[Long] {
            override def reduce(v1: Long, v2: Long): Long = {
              v1 + v2
            }
          }, createTypeInformation[Long])
          speedCount = getRuntimeContext.getReducingState(desc)
        }

        override def map(v1: (String, Long)): (String, Long) = {
          speedCount.add(v1._2)
          (v1._1, speedCount.get())
        }
      }).print()
    env.execute()
  }
}