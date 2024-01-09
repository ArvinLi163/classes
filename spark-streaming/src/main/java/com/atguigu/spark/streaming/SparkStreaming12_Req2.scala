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
 * @description: 实时统计每天各地区各城市各广告的点击总流量
 * @author: bansheng
 * @date: 2024/01/02 15:03
 * */
object SparkStreaming12_Req2 {
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
    val reduceDS: DStream[((String, String, String, String), Int)] = adClickData.map(
      data => {
        val spdf = new SimpleDateFormat("yyyy-MM-dd")
        val day = spdf.format(new Date(data.ts.toLong))
        val area = data.area
        val city = data.city
        val ad = data.ad
        ((day, area, city, ad), 1)
      }
    ).reduceByKey(_ + _)

    reduceDS.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter => {
            val conn: Connection = JDBCUtil.getConnection
            val pstat: PreparedStatement = conn.prepareStatement(
              """
                |insert into area_city_ad_count(dt,area,city,adid,count)
                |values( ?,?,?,?,?)
                |on DUPLICATE KEY
                |UPDATE count = count + ?
                |""".stripMargin)
            iter.foreach {
              case ((day, area, city, ad), sum) => {
                pstat.setString(1, day)
                pstat.setString(2, area)
                pstat.setString(3, city)
                pstat.setString(4, ad)
                pstat.setInt(5, sum)
                pstat.setInt(6, sum)
                pstat.executeUpdate()
              }
            }
            conn.close()
          }
        )
      }
    )

    //启动采集器
    ssc.start()
    //等待采集器的关闭
    ssc.awaitTermination()
  }

  case class AdClickData(ts: String, area: String, city: String, user: String, ad: String)
}
