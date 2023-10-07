package com.atguigu.spark.core.rdd.create

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/14 9:46
 * */
object Spark01_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    //设置分区数
    conf.set("spark.default.parallelism", "5")
    val sc: SparkContext = new SparkContext(conf)
    //TODO 创建RDD
    //并行度&分区
    //2个分区
    // scheduler.conf.getInt("spark.default.parallelism", totalCores)
    //默认值取totalCores，从配置参数里面取值，表示当前环境的最大可用核数
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //   val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    rdd.saveAsTextFile("output")
    //TODO 关闭
    sc.stop()
  }
}
