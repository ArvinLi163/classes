package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @description: 无状态转化操作-join
 * @author: bansheng
 * @date: 2024/01/02 15:03
 * */
object SparkStreaming06_State_Join {
  def main(args: Array[String]): Unit = {
    //创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(conf, Seconds(3))

    //具体逻辑
    //监听端口数据
    val ds999: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val ds888: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8888)
    val map999: DStream[(String, Int)] = ds999.map((_, 9))
    val map888: DStream[(String, Int)] = ds888.map((_, 8))
    val resultDS: DStream[(String, (Int, Int))] = map999.join(map888)
    resultDS.print()


    //启动采集器
    ssc.start()
    //等待采集器的关闭
    ssc.awaitTermination()
  }

}
