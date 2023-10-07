package com.atguigu.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/11 20:16
 * */
object Spark03_WordCount {
  def main(args: Array[String]): Unit = {
    //1.建立连接
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)
    //2.具体操作
    val lines: RDD[String] = sc.textFile("datas")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //hello =>(hello,1) scala =>(scala,1)
    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))
    //spark提供了更多的功能，将分组和聚合放在了一起
    //wordToOne.reduceByKey((x, y) => x + y)
    //相同的key,可以对value进行聚合
    val count: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    //将转换后的结果进行采集
    val array: Array[(String, Int)] = count.collect()
    array.foreach(println)
    //3.关闭连接
    sc.stop()
  }
}
