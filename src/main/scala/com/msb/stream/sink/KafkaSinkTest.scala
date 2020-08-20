package com.msb.stream.sink

import java.lang
import java.util.Properties

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.mutable.ListBuffer

object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.socketTextStream("node01", 8888)

    val props = new Properties()
    props.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092")

    stream.flatMap(line => {
      val rest = new ListBuffer[(String, Int)]
      line.split(" ").foreach(word => rest += ((word, 1)))
      rest
    })
      .keyBy(_._1)
      .reduce((v1: (String, Int), v2: (String, Int)) => {
        (v1._1, v1._2 + v2._2)
      })
      .addSink(new FlinkKafkaProducer[(String, Int)](
        "wc",
        new KafkaSerializationSchema[(String, Int)] {
          override def serialize(t: (String, Int), aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
            new ProducerRecord[Array[Byte], Array[Byte]]("wc", t._1.getBytes(), t._2.toString.getBytes())
          }
        },
        props,
        Semantic.EXACTLY_ONCE
      ))
    env.execute()
  }
}