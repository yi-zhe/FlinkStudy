package com.msb.stream.state

import org.apache.flink.api.common.functions.{AggregateFunction, RichMapFunction}
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

/**
 * 统计所有车的速度总和
 */
object AggregateStateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("node01", 8888)
    stream.map(data => {
      val splits = data.split(" ")
      (splits(0), splits(1).toLong)
    }).keyBy(_._1)
      .map(new RichMapFunction[(String, Long), (String, Long)] {

        private var speedCount: AggregatingState[Long, Long] = _

        override def open(parameters: Configuration): Unit = {
          val desc = new AggregatingStateDescriptor[Long, Long, Long]("reduce", new AggregateFunction[Long, Long, Long] {
            // 初始化一个累加器
            override def createAccumulator(): Long = 0

            // 每来一条数据调用一次
            override def add(in: Long, acc: Long): Long = in + acc

            override def getResult(acc: Long): Long = acc

            override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
          }, createTypeInformation[Long])
          speedCount = getRuntimeContext.getAggregatingState(desc)
        }

        override def map(v1: (String, Long)): (String, Long) = {
          speedCount.add(v1._2)
          (v1._1, speedCount.get())
        }
      }).print()
    env.execute()
  }
}