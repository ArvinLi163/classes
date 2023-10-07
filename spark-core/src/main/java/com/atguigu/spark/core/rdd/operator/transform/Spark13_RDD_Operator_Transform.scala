package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/16 9:25
 * */
object Spark13_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //连接环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)
    //TODO 双value类型
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6))
    //交集
    val rdd3: RDD[Int] = rdd1.intersection(rdd2)
    println(rdd3.collect().mkString(","))
    //并集
    val rdd4: RDD[Int] = rdd1.union(rdd2)
    println(rdd4.collect().mkString(","))
    //差集 [1,2]
    val rdd5: RDD[Int] = rdd1.subtract(rdd2)
    println(rdd5.collect().mkString(","))
    //拉链 [1,3] [2,4] [3,5] [4,6]
    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))
    //关闭环境
    sc.stop()
  }
}
