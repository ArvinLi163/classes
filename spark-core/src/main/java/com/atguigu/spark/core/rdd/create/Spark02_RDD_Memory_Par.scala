package com.atguigu.spark.core.rdd.create

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/14 9:46
 * */
object Spark02_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(conf)
    //TODO 创建RDD
    //textFile作为数据源
    //defaultMinPartitions 默认最小分区数
    //math.min(defaultParallelism, 2) 默认为2
    //底层用的是hadoop的分区方式
    //7/2=3...1(分区)  3个分区
    val rdd: RDD[String] = sc.textFile("datas/1.txt",2)
    rdd.saveAsTextFile("output")
    //TODO 关闭
    sc.stop()
  }
}
