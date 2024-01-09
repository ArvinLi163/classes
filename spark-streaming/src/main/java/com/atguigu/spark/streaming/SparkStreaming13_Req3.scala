package com.atguigu.spark.streaming

import com.atguigu.spark.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Date


/**
 * @description: 最近一小时广告点击量
 * @author: bansheng
 * @date: 2024/01/02 15:03
 * */
object SparkStreaming13_Req3 {
  def main(args: Array[String]): Unit = {
    //创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(conf, Seconds(5))
    //定义kafka参数
    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "lgy",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
    //kafka工具类 第二个参数是节点策略，第三个参数是消费者策略
    val consumerRecord: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("lgy"), kafkaParams))
    val adClickData: DStream[AdClickData] = consumerRecord.map(
      kafkaData => {
        val data: String = kafkaData.value()
        val datas: Array[String] = data.split(" ")
        AdClickData(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )

    val reduceDS: DStream[(Long, Int)] = adClickData.map(
      data => {
        val ts = data.ts.toLong
        val newTS = ts / 10000 * 10000
        (newTS, 1)
      }
    ).reduceByKeyAndWindow((x:Int,y:Int)=>{x+y}, Seconds(60), Seconds(10))
    reduceDS.print()


    //启动采集器
    ssc.start()
    //等待采集器的关闭
    ssc.awaitTermination()
  }

  case class AdClickData(ts: String, area: String, city: String, user: String, ad: String)
}
