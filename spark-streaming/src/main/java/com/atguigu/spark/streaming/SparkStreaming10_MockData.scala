package com.atguigu.spark.streaming


import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.{Properties, Random}
import scala.collection.mutable.ListBuffer


/**
 * @description: 模拟数据
 * @author: bansheng
 * @date: 2024/01/02 15:03
 * */
object SparkStreaming10_MockData {
  def main(args: Array[String]): Unit = {
    //配置对象
    val prop = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](prop)
    while (true) {
      mockData().foreach(
        data => {
          //向Kafka中生成数据
          val record = new ProducerRecord[String, String]("lgy", data)
          producer.send(record)
          println(data)
        }
      )
      Thread.sleep(2000)
    }

    //Application=>Kafka=>SparkStreaming=>Analysis

  }

  def mockData() = {
    //生成模拟数据
    //格式：timestamp area city userid adid
    //含义：时间戳 区域 城市 用户 广告
    val list = ListBuffer[String]()
    val areaList = ListBuffer[String]("华北", "华南", "华东")
    val cityList = ListBuffer[String]("北京", "上海", "深圳")
//    for (i <- 1 to 30) {
    for (i <- 1 to new Random().nextInt(50)) {
      val area = areaList(new Random().nextInt(3))
      val city = cityList(new Random().nextInt(3))
      val userid = new Random().nextInt(6) + 1
      val adid = new Random().nextInt(6) + 1
      list.append(s"${System.currentTimeMillis()} ${area} ${city} ${userid} ${adid}")
    }
    list
  }

}
