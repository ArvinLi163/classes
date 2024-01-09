package com.atguigu.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @description: 消费kafka生产的数据
 * @author: bansheng
 * @date: 2024/01/02 15:03
 * */
object SparkStreaming11_Req1 {
  def main(args: Array[String]): Unit = {
    //创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(conf, Seconds(3))
    //定义kafka参数
    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "lgy",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
    //kafka工具类 第二个参数是节点策略，第三个参数是消费者策略
    val consumerRecord: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String,String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Set("lgy"), kafkaParams))
    consumerRecord.map(_.value()).print()
    //启动采集器
    ssc.start()
    //等待采集器的关闭
    ssc.awaitTermination()
  }
}
