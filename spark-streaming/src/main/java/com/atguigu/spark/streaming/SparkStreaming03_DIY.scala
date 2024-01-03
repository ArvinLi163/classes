package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Random

/**
 * @description: rddQueue
 * @author: bansheng
 * @date: 2024/01/02 15:03
 * */
object SparkStreaming03_DIY {
  def main(args: Array[String]): Unit = {
    //创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(conf, Seconds(3))

    val messageDS: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver())
    messageDS.print()
    //启动采集器
    ssc.start()
    //等待采集器的关闭
    ssc.awaitTermination()
  }


  /*
  自定义数据采集器
  1.继承Receiver,定义泛型，传递参数
  2.重写方法
   */
  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    private var flag=true
    override def onStart(): Unit = {
        new Thread(new Runnable {
          override def run(): Unit = {
            while(flag){
              val message: String = "采集的数据为："+ new Random().nextInt(10).toString
              store(message)
              Thread.sleep(500)
            }
          }
        }).start()
    }

    override def onStop(): Unit = {
      flag = true
    }
  }

}
