package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/16 9:25
 * */
object Spark17_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //连接环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)
    //TODO key-value类型
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1),
      ("a", 2),
      ("a", 3),
      ("a", 4)
    ),2)
    //第一个参数2是初始化，用于和第一个key的value值进行的操作
    val rdd1: RDD[(String, Int)] = rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y), //分区内进行的操作
      (x, y) => x + y //分区间进行的操作
    )
    rdd1.collect().foreach(println)
    //关闭环境
    sc.stop()
  }
}
