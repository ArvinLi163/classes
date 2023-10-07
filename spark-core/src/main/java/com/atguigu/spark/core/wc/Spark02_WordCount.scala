package com.atguigu.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/11 20:16
 * */
object Spark02_WordCount {
  def main(args: Array[String]): Unit = {
    //1.建立连接
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)
    //2.具体操作
    val lines: RDD[String] = sc.textFile("datas")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //hello =>(hello,1) scala =>(scala,1)
    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))
    val groupCount: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(t => t._1)
    val count: RDD[(String, Int)] = groupCount.map {
      case (word, list) => list.reduce((t1, t2) => (t1._1, t1._2 + t2._2))
    }
    //2.5将转换后的结果进行采集
    val array: Array[(String, Int)] = count.collect()
    array.foreach(println)
    //3.关闭连接
    sc.stop()
  }
}
