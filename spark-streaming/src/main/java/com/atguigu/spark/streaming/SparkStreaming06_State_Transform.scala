package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @description: 无状态转化操作-tranform
 * @author: bansheng
 * @date: 2024/01/02 15:03
 * */
object SparkStreaming06_State_Transform {
  def main(args: Array[String]): Unit = {
    //创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(conf, Seconds(3))

    //具体逻辑
    //监听端口数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val lineDS: DStream[String] = lines.transform(rdd => rdd)
    lineDS.print()


    //启动采集器
    ssc.start()
    //等待采集器的关闭
    ssc.awaitTermination()
  }

}
