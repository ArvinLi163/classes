package com.atguigu.spark.core.rdd.depend

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/10/09 15:10
 * */
object Spark01_RDD_Depend {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("datas/word.txt")
    println(lines.toDebugString)
    println("***********************")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    println(words.toDebugString)
    println("***********************")
    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))
    println(wordToOne.toDebugString)
    println("***********************")
    val count: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    println(count.toDebugString)
    println("***********************")
    //将转换后的结果进行采集
    val array: Array[(String, Int)] = count.collect()
    array.foreach(println)

    sc.stop()
  }
}
