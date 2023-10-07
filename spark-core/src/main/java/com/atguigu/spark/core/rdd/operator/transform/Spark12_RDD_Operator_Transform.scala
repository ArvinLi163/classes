package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/16 9:25
 * */
object Spark12_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //连接环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)
    //TODO sortBy
    val rdd: RDD[Int] = sc.makeRDD(List(5, 3,6, 2, 1, 4), 2)
    val newRdd: RDD[Int] = rdd.sortBy(num => num)
    newRdd.saveAsTextFile("output")

    //关闭环境
    sc.stop()
  }
}
