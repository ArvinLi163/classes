package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/**
 * @description: 数据恢复
 * @author: bansheng
 * @date: 2024/01/02 15:03
 * */
object SparkStreaming09_Resume {
  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("cp", () => {
      //创建环境
      val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
      val ssc = new StreamingContext(conf, Seconds(3))


      val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
      val wordToOne: DStream[(String, Int)] = lines.map((_, 1))


      val word: DStream[(String, Int)] = wordToOne.window(Seconds(6), Seconds(6))
      val wordToCount: DStream[(String, Int)] = word.reduceByKey(_ + _)
      wordToCount.print()
      ssc
    })

    ssc.checkpoint("cp")
    //启动采集器
    ssc.start()
    //等待采集
    ssc.awaitTermination()

  }

}
