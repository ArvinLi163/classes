package com.atguigu.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/10/09 16:18
 * */
object Spark01_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    val list: List[String] = List("Hello Spark", "Hello Scala")
    val RDD: RDD[String] = sc.makeRDD(list)
    val flatRDD: RDD[String] = RDD.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = flatRDD.map(word => (word, 1))
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)
    println("*********************")
    val list1: List[String] = List("Hello Spark", "Hello Scala")
    val RDD1: RDD[String] = sc.makeRDD(list1)
    val flatRDD1: RDD[String] = RDD1.flatMap(_.split(" "))
    val mapRDD1: RDD[(String, Int)] = flatRDD1.map(word => (word, 1))
    val groupRDD1: RDD[(String, Iterable[Int])] = mapRDD1.groupByKey()
    groupRDD1.collect().foreach(println)
    sc.stop()
  }
}
