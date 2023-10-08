package com.atguigu.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/11 20:16
 * */
object Spark04_WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)
    wordCount1(sc)
    sc.stop()
  }

  def wordCount1(sc: SparkContext) = {
    val datas: RDD[String] = sc.makeRDD(List("Hello Spark", "Hello Scala"))
    val words: RDD[String] = datas.flatMap(_.split(" "))
    val group: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    val wordCount: RDD[(String, Int)] = group.mapValues(iter => iter.size)
  }
}
