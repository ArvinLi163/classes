package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2024/01/02 15:03
 * */
object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    //创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(conf, Seconds(3))

    //具体逻辑
    //监听端口数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val line: DStream[String] = lines.flatMap(_.split(" "))
    println(line)
    val wordToOne: DStream[(String, Int)] = line.map((_, 1))
    val wordToCount: DStream[(String, Int)] = wordToOne.reduceByKey(_ + _)
    wordToCount.print()

    //ssc.stop()
    //启动采集器
    ssc.start()
    //等待采集器的关闭
    ssc.awaitTermination()
  }

}
