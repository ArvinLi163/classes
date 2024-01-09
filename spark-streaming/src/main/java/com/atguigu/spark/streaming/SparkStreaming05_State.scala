package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream,ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @description: 有状态转换操作
 * @author: bansheng
 * @date: 2024/01/02 15:03
 * */
object SparkStreaming05_State {
  def main(args: Array[String]): Unit = {
    //创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(conf, Seconds(3))

    //设置检查点
    ssc.checkpoint("./cp")
    val datas: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val wordToOne: DStream[(String, Int)] = datas.map((_, 1))
    //seq代表键值对中的value值，buff代表缓冲区中的value值
    val wordToCount: DStream[(String, Int)] = wordToOne.updateStateByKey(
      (seq: Seq[Int], buff: Option[Int]) => {
        val newCount = buff.getOrElse(0) + seq.sum
        Option(newCount)
      })
    wordToCount.print()
    //启动采集器
    ssc.start()
    //等待采集器的关闭
    ssc.awaitTermination()
  }
}
