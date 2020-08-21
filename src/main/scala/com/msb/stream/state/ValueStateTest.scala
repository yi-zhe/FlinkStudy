package com.msb.stream.state

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object ValueStateTest {

  case class CarInfo(carId: String, speed: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("node01", 8888)
    // 车牌号 速度
    stream.map(data => {
      val splits = data.split(" ")
      CarInfo(splits(0), splits(1).toInt)
    })
      .keyBy(_.carId)
      .map(new RichMapFunction[CarInfo, String] {
        private var lastTempSpeed: ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
          val desc = new ValueStateDescriptor[Long]("lastSpeed", createTypeInformation[Long])
          lastTempSpeed = getRuntimeContext.getState(desc)
        }

        override def close(): Unit = {

        }

        override def map(value: CarInfo): String = {
          val lastSpeed = lastTempSpeed.value()
          lastTempSpeed.update(value.speed)
          // 不用考虑时间差吗
          if (lastSpeed != 0 && value.speed - lastSpeed > 30) {
            "over speed " + value.toString
          } else {
            value.carId
          }
        }
      }).print()
    env.execute()
  }
}