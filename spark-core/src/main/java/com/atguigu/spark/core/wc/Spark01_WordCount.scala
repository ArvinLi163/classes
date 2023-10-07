package com.atguigu.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/11 20:16
 * */
object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    //1.建立连接
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)
    //2.具体操作
    //2.1读取文件，获取每一行数据
    val lines: RDD[String] = sc.textFile("datas")
    //2.2将每一行的数据拆成多个单词 （扁平化）
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //2.3对单词进行分组
    val groupCount: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    //2.4对每个组别中的单词进行转换
    val count: RDD[(String, Int)] = groupCount.map {
      case (word, list) => (word, list.size)
    }
    //2.5将转换后的结果进行采集
    val array: Array[(String, Int)] = count.collect()
    array.foreach(println)
    //3.关闭连接
    sc.stop()
  }
}
