package com.msb.stream.source

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source

object FlinkKafkaProducer {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092")
    properties.setProperty("key.serializer", classOf[StringSerializer].getName)
    properties.setProperty("value.serializer", classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](properties)
    val source = Source.fromFile("./data/carFlow_all_column_test.txt")
    //不要轻易调用getLines
    val iterator = source.getLines()
    for (i <- 1 to 100) {
      for (element <- iterator) {
        //kv mq
        val splits = element.split(",")
        val monitorId = splits(0).replace("'", "")
        val carId = splits(2).replace("'", "")
        val timestamp = splits(4).replace("'", "")
        val speed = splits(6)
        val builder = new StringBuilder
        val info = builder.append(monitorId).append("\t").append(carId).append("\t").append(timestamp).append("\t").append(speed)
        producer.send(new ProducerRecord[String, String]("flink-kafka", i + "", info.toString))
        Thread.sleep(500)
      }
    }
    source.close()
  }
}
