package com.atguigu.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/10/07 16:56
 * */
object Spark04_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,1,1,3))
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3)))
    //TODO 行动算子
    val intToLong: collection.Map[Int, Long] = rdd.countByValue()
    println(intToLong)

    //统计a出现的次数
    val map: collection.Map[String, Long] = rdd1.countByKey()
    println(map)
    sc.stop()
  }
}
