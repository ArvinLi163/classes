package com.atguigu.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/10/09 16:18
 * */
object Spark03_RDD_Persist {
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
    //内存持久化
    //mapRDD.cache()
    //硬盘持久化
    mapRDD.persist(StorageLevel.DISK_ONLY)
    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)
    sc.stop()
  }
}
