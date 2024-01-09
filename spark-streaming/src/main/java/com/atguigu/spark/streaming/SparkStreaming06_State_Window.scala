package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @description: 无状态转化操作-window
 * @author: bansheng
 * @date: 2024/01/02 15:03
 * */
object SparkStreaming06_State_Window {
  def main(args: Array[String]): Unit = {
    //创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(conf, Seconds(3))

    //具体逻辑
    //监听端口数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val wordToOne: DStream[(String, Int)] = lines.map((_, 1))

    //开窗 开窗周期是采集周期的整数倍
    //划窗周期默认是一个采集周期，为了避免数据的重复采集，修改为开窗周期
    val word: DStream[(String, Int)] = wordToOne.window(Seconds(6),Seconds(6))
    val wordToCount: DStream[(String, Int)] = word.reduceByKey(_ + _)
    wordToCount.print()
    //启动采集器
    ssc.start()
    //等待采集器的关闭
    ssc.awaitTermination()
  }

}
