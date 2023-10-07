package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/16 9:25
 * */
object Spark23_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //连接环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)
    //TODO key-value类型
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 4), ("b", 5), ("c", 6)
      )
    )

    rdd1.cogroup(rdd2).collect().foreach(println)

    //关闭环境
    sc.stop()
  }
}
