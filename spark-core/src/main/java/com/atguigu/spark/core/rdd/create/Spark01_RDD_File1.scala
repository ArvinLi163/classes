package com.atguigu.spark.core.rdd.create

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/14 9:46
 * */
object Spark01_RDD_File1 {
  def main(args: Array[String]): Unit = {
    //准备环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(conf)
    //创建RDD
    //从文件中创建rdd,将文件中的数据作为数据源
    //wholeTextFiles 以行为单位 读取的结果是一个元组，第一个元素代表的是文件的路径，第二个元素是文件中的数据
    val rdd: RDD[(String, String)] = sc.wholeTextFiles("datas")
    rdd.collect().foreach(println)
    //关闭环境
    sc.stop()
  }
}
