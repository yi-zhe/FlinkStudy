package com.msb.stream.transformation

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputOperator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("node01", 8888)
    // 定义side output stream的tag标签
    val gtTag = new OutputTag[String]("gt")
    // process 可以理解为是flink的一个算子
    val processStream = stream.process(new ProcessFunction[String, String] {
      // 处理每一个元素
      override def processElement(value: String, context: ProcessFunction[String, String]#Context, collector: Collector[String]): Unit = {
        try {
          val longVar = value.toLong
          if (longVar > 100) {
            // 往主流发射
            collector.collect(value)
          } else {
            // 往侧流发射
            context.output(gtTag, value)
          }
        } catch {
          case e => e.getMessage
            context.output(gtTag, value)
        }
      }
    })

    // 获取测流数据
    val sideStream = processStream.getSideOutput(gtTag)
    sideStream.print("sideStream")
    // 默认print打印的是主流数据
    processStream.print("mainStream")

    env.execute()
  }
}