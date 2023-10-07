package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/16 9:25
 * */
object Spark07_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //连接环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)
    //TODO filter
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //只要奇数
    val filterRDD: RDD[Int] = rdd.filter(num => num % 2 != 0)
    filterRDD.collect().foreach(println)

    //关闭环境
    sc.stop()
  }
}
