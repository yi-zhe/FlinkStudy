package com.msb.stream.sink

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ListBuffer

/**
 * MySQL不支持幂等操作
 * 需要代码实现
 * flink 1
 * flink 2 而不是两条flink 1
 * 先update 如果失败再insert就可以实现幂等了
 *
 * Flink没有原生支持MySQL所以需要自定义Sink
 * 导入MySQL的驱动包
 */
object MySQLSinkTest {
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
      .addSink(new RichSinkFunction[(String, Int)] {
        // 线程开始执行业务逻辑之前会调用 一次

        var conn: Connection = _
        var updatePst: PreparedStatement = _
        var insertPst: PreparedStatement = _

        // 生产环境可以做一个集合, 达到一定的量之后, 才触发MySQL更新

        override def open(parameters: Configuration): Unit = {
          conn = DriverManager.getConnection("jdbc://mysql://192.168.101.199:3306/test", "root", "123124")
          updatePst = conn.prepareStatement("UPDATE wc SET count = ? WHERE word = ?")
          insertPst = conn.prepareStatement("INSERT INTO wc VALUES(?,?)")
        }

        // 线程执行业务逻辑之后会调用 一次
        override def close(): Unit = {
          updatePst.close()
          insertPst.close()
          conn.close()
        }

        // 每有一个元素调用一次
        override def invoke(value: (String, Int), context: SinkFunction.Context[_]): Unit = {
          updatePst.setInt(1, value._2)
          updatePst.setString(2, value._1)
          updatePst.execute()
          if (updatePst.getUpdateCount == 0) {
            insertPst.setString(1, value._1)
            insertPst.setInt(2, value._2)
            insertPst.execute()
          }
        }
      })
    env.execute()
  }
}