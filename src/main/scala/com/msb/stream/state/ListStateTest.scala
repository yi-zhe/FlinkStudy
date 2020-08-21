package com.msb.stream.state

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringSerializer
import scala.collection.JavaConverters._

object ListStateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置连接kafka的配置信息
    val props = new Properties()
    //注意   sparkstreaming + kafka（0.10之前版本） receiver模式  zookeeper url（元数据）
    props.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092")
    props.setProperty("group.id", "flink-kafka")
    props.setProperty("key.deserializer", classOf[StringSerializer].getName)
    props.setProperty("value.deserializer", classOf[StringSerializer].getName)

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val stream = env.addSource(new FlinkKafkaConsumer[String]("flink-kafka", new SimpleStringSchema(), props))
    stream.map(data => {
      val arr = data.split("\t")
      val time = format.parse(arr(2)).getTime
      (arr(0), arr(1), time, arr(3).toLong)
    }).keyBy(_._2)
      .map(new RichMapFunction[(String, String, Long, Long), (String, String)] {

        private var carInfos: ListState[(String, Long)] = _

        override def open(parameters: Configuration): Unit = {
          val desc = new ListStateDescriptor[(String, Long)]("list", createTypeInformation[(String, Long)])
          carInfos = getRuntimeContext.getListState(desc)
        }

        override def close(): Unit = {

        }

        override def map(elem: (String, String, Long, Long)): (String, String) = {
          carInfos.add((elem._1, elem._3))
          val seq = carInfos.get().asScala.seq
          val sortList = seq.toList.sortBy(_._2)
          val builder = new StringBuilder
          for (elem <- sortList) {
            builder.append(elem._1).append("\t")
          }
          (elem._2, builder.toString())
        }
      }).print()

    env.execute()
  }
}