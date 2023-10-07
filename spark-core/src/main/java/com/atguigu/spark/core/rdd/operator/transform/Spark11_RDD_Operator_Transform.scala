package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/16 9:25
 * */
object Spark11_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //连接环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)
    //TODO 添加分区
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
   //coalesce可以增加分区，默认shuffle是false，是没有任何意义的
    //如果要扩大分区数量，必须将shuffle设置为true
    //val newRdd: RDD[Int] = rdd.coalesce(3, true)

    //spark提供了另外一个算子来添加分区
    val newRdd: RDD[Int] = rdd.repartition(3)
    newRdd.saveAsTextFile("output")

    //关闭环境
    sc.stop()
  }
}
