package com.atguigu.spark.core.rdd.create

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/14 9:46
 * */
object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {
    //准备环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(conf)
    //创建RDD
    //从内存中创建rdd,将集合作为数据源
    val seq: Seq[Int] = Seq(1, 2, 3, 4)
    //parallelize 并行
    //val rdd: RDD[Int] = sc.parallelize(seq)
    val rdd: RDD[Int] = sc.makeRDD(seq)
    rdd.collect().foreach(println)
    //关闭
    sc.stop()
  }
}
