package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/16 9:25
 * */
object Spark24_RDD_Req {
  def main(args: Array[String]): Unit = {
    //连接环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)
    //TODO 案例实操
    //1.读取相关的数据 时间戳，省份，城市，用户，广告   中间字段使用空格分隔
    val dataRDD: RDD[String] = sc.textFile("datas/agent.log")
    //2.把需要的数据保留下来并进行结构的转换
    val mapRDD: RDD[((String, String), Int)] = dataRDD.map(
      line => {
        val data: Array[String] = line.split(" ")
        ((data(1), data(4)), 1)
      }
    )
    //3. 将转换结构后的数据，进行分组聚合
    val reduceRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)
    //4. 将聚合的结果进行结构的转换
    val newMapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case ((pre, ad), sum) => {
        (pre, (ad, sum))
      }
    }
    //5.按照省份进行分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = newMapRDD.groupByKey()
    //6.排序，取前三
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )
    //7.采集并打印到控制台上
    resultRDD.collect().foreach(println)

    //关闭环境
    sc.stop()
  }
}
