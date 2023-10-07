package com.atguigu.spark.core.rdd.create

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/14 9:46
 * */
object Spark01_RDD_Memory_Par1 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(conf)
    //TODO 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),3)
    //val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    rdd.saveAsTextFile("output")
    //TODO 关闭
    sc.stop()
  }
}
