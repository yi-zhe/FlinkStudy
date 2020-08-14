package com.msb.stream.source

import org.apache.flink.api.java.io.TextValueInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * flume->hdfs->flink 实时ETL->入仓
 *
 * flume->kafka->flink 实时ETL->入仓
 */
object ReadHDFS {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val filePath = "hdfs://mycluster/flink/data"
    val format = new TextValueInputFormat(new Path(filePath))
    try {
      val stream = env.readFile(format, filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 10000)
      stream.print()
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    env.execute()
  }
}
