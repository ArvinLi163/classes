package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/16 9:25
 * */
object Spark16_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //连接环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)
    //TODO key-value类型
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1),
      ("a", 2),
      ("a", 3),
      ("b", 4)
    ))
    val rdd1: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    rdd1.collect().foreach(println)

    val rdd2: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)
    rdd2.collect().foreach(println)
    //关闭环境
    sc.stop()
  }
}
