package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @description: rddQueue
 * @author: bansheng
 * @date: 2024/01/02 15:03
 * */
object SparkStreaming02_Queue {
  def main(args: Array[String]): Unit = {
    //创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(conf, Seconds(3))

    val rddQueue = new mutable.Queue[RDD[Int]]()
    val inputStream: InputDStream[Int] = ssc.queueStream(rddQueue, oneAtATime = false)
    val mappedStream: DStream[(Int, Int)] = inputStream.map((_, 1))
    val reduceStream: DStream[(Int, Int)] = mappedStream.reduceByKey(_ + _)
    //打印结果
    reduceStream.print()


    //启动采集器
    ssc.start()
    for (i <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }
    //等待采集器的关闭
    ssc.awaitTermination()
  }

}
