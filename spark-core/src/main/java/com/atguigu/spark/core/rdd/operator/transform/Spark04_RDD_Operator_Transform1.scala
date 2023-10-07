package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/16 9:25
 * */
object Spark04_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    //连接环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)
    //TODO flatMap
    val m1: RDD[String] = sc.makeRDD(List("Hello spark", "Hello scala"))
    val m2: RDD[String] = m1.flatMap(s => s.split(" "))
    m2.collect().foreach(println)
    //关闭环境
    sc.stop()
  }
}
