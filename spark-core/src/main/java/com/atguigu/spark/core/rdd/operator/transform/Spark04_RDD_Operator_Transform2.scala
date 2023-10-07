package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/16 9:25
 * */
object Spark04_RDD_Operator_Transform2 {
  def main(args: Array[String]): Unit = {
    //连接环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)
    //TODO flatMap
//    val rdd: RDD[String] = sc.makeRDD(List(List(1, 2), 3, List(4, 5)))
//    val flatRdd = rdd.flatMap(
//      data => {
//        data match {
//          case list: List[_] => list
//          case dat => List(dat)
//        }
//      }
//    )
//    flatRdd.collect().foreach(println)
    //关闭环境
    sc.stop()
  }
}
