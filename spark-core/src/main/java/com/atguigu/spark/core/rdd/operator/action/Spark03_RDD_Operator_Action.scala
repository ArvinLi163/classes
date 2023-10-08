package com.atguigu.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/10/07 16:56
 * */
object Spark03_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    //TODO 行动算子
    //aggregateByKey:初始值只会参与分区内的计算
    //aggregate:初始值会参与分区内计算，并且参与分区间计算
    //val result: Int = rdd.aggregate(10)(_ + _, _ + _)

    //分区内、分区间的计算规则一样时
    val result: Int = rdd.fold(10)(_ + _)
    println(result)
    sc.stop()
  }
}
