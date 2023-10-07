package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/16 9:25
 * */
object Spark06_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    //连接环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)
    //TODO groupBy
    val rdd: RDD[String] = sc.textFile("datas/apache.log")
    val timeRdd: RDD[(String, Iterable[(String, Int)])] = rdd.map(
      line => {
        val datas = line.split(" ")
        //获取关于时间的字符串
        val time = datas(3)
        //获取小时
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val date = sdf.parse(time)
        val sdf1 = new SimpleDateFormat("HH")
        val hour: String = sdf1.format(date)
        (hour, 1)
      }
    ).groupBy(_._1)
    //模式匹配
    //统计每个时间段出现的次数
    timeRdd.map {
      case (hour, iter) => {
        (hour, iter.size)
      }
    }.collect().foreach(println)
    //关闭环境
    sc.stop()
  }
}
