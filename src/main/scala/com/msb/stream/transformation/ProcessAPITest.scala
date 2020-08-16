package com.msb.stream.transformation

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

object ProcessAPITest {

  case class CarInfo(carId: String, speed: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("node01", 8888)
    stream.map(data => {
      val splits = data.split(" ")
      val carId = splits(0)
      var speed = splits(1).toLong
      CarInfo(carId, speed)
    })
      .keyBy(_.carId)
      .process(new KeyedProcessFunction[String, CarInfo, String] {
        override def processElement(value: CarInfo, context: KeyedProcessFunction[String, CarInfo, String]#Context, collector: Collector[String]): Unit = {
          val currentTime = context.timerService().currentProcessingTime()
          if (value.speed > 100) {
            val timerTime = currentTime + 2 * 1000
            context.timerService().registerProcessingTimeTimer(timerTime)
          }
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, CarInfo, String]#OnTimerContext, out: Collector[String]): Unit = {
          var warnMsg = "warn... time:" + timestamp + " carId:" + ctx.getCurrentKey
          out.collect(warnMsg)
        }
      }).print()

    env.execute()
  }
}