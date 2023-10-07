package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/16 9:25
 * */
object Spark01_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    //连接环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)
    //TODO 算子 -map
    val rdd: RDD[String] = sc.textFile("datas/apache.log")
    val makeRDD: RDD[String] = rdd.map((line: String) => {
      val datas: Array[String] = line.split(" ")
      datas(6)
    })
    //
    makeRDD.collect().foreach(println)
    //关闭环境
    sc.stop()
  }
}
