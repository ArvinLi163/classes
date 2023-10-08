package com.atguigu.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/10/07 16:56
 * */
object Spark02_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //TODO 行动算子
    //val i: Int = rdd.reduce(_ + _)
    //println(i)

    //collect:该方法是将不同分区的数据按照分区顺序采集到Driver端内存中，形成数组
    //val ints: Array[Int] = rdd.collect()
    //println(ints.mkString(","))

    //count:数据源中数据的个数
    val cnt: Long = rdd.count()
    println(cnt)

    //first:取第一个元素
    val first: Int = rdd.first()
    println(first)

    //take:取出前N个元素
    val ints: Array[Int] = rdd.take(3)
    println(ints.mkString(","))

    //takeOrdered:取出排序之后的前N个元素
    val rdd1: RDD[Int] = sc.makeRDD(List(3, 4, 2, 1))
    val orderArr: Array[Int] = rdd1.takeOrdered(3)
    println(orderArr.mkString(","))

    sc.stop()
  }
}
