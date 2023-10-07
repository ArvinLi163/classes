package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/16 9:25
 * */
object Spark02_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    //连接环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)
    //TODO 算子 -mapPartitions
    //以分区为单位，但是不会释放资源，容易导致内存溢出
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    //求每个分区最大值
    val makeRDD: RDD[Int] = rdd.mapPartitions((iter: Iterator[Int]) => {
      List(iter.max).iterator //将最大值封装成一个迭代器
    })
    makeRDD.collect().foreach(println)
    //关闭环境
    sc.stop()
  }
}
