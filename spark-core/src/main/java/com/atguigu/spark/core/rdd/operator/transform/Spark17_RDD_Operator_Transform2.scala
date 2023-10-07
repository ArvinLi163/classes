package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/16 9:25
 * */
object Spark17_RDD_Operator_Transform2 {
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
    ), 2)
    //进行聚合操作时，分区内和分区间所采取的规则相同时，可以使用一下方法
    rdd.foldByKey(0)(_ + _).collect().foreach(println)
    //关闭环境
    sc.stop()
  }
}
