package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/16 9:25
 * */
object Spark05_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //连接环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)
    //TODO glom
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    //Int=>Array
    val glomRdd: RDD[Array[Int]] = rdd.glom()
    glomRdd.collect().foreach(data => println(data.mkString(",")))
    //关闭环境
    sc.stop()
  }
}
