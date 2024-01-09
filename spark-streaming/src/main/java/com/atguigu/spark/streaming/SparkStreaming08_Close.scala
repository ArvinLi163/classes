package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/**
 * @description: 优雅关闭
 * @author: bansheng
 * @date: 2024/01/02 15:03
 * */
object SparkStreaming08_Close {
  def main(args: Array[String]): Unit = {
    //创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(conf, Seconds(3))


    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val wordToOne: DStream[(String, Int)] = lines.map((_, 1))


    val word: DStream[(String, Int)] = wordToOne.window(Seconds(6), Seconds(6))
    val wordToCount: DStream[(String, Int)] = word.reduceByKey(_ + _)
    wordToCount.print()
    //启动采集器
    ssc.start()

    //如果想要关闭采集器，那么需要创建新的线程，而且需要在第三方程序中增加关闭状态
    new Thread(
      new Runnable {
        override def run(): Unit = {
         /* while (true) {
            if (true) {
              //获取SparkStreaming的状态
              val state: StreamingContextState = ssc.getState()
              if (state == StreamingContextState.ACTIVE){
                //优雅地关闭
                //计算节点不再接收新的数据，而是将现有的数据处理完毕，然后关闭
                ssc.stop(true, true)
              }
            }
            Thread.sleep(5000)
          } */
          Thread.sleep(5000)
          //获取SparkStreaming的状态
          val state: StreamingContextState = ssc.getState()
          if (state == StreamingContextState.ACTIVE) {
            //优雅地关闭
            //计算节点不再接收新的数据，而是将现有的数据处理完毕，然后关闭
            ssc.stop(true, true)
          }
          System.exit(0)
        }
      }
    ).start()


    //等待采集
    ssc.awaitTermination()

  }

}
