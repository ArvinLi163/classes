package com.atguigu.spark.core.rdd.create

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/14 9:46
 * */
object Spark01_RDD_File {
  def main(args: Array[String]): Unit = {
    //准备环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(conf)
    //创建RDD
    //从文件中创建rdd,将文件中的数据作为数据源
    //绝对路径、相对路径都是可以的
    //val rdd: RDD[String] = sc.textFile("D:\\IDEA\\bigdata_learning\\spark\\classes\\datas\\1.txt")
    //val rdd: RDD[String] = sc.textFile("datas/1.txt")
    //可以是目录
    //val rdd: RDD[String] = sc.textFile("datas")
    //可以使用通配符
    val rdd: RDD[String] = sc.textFile("datas/1*.txt")
    //可以是分布式文件系统
    //val rdd: RDD[String] = sc.textFile("hdfs://hadoop102:8020/dir")
    rdd.collect().foreach(println)
    //关闭环境
    sc.stop()
  }
}
