package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/16 9:25
 * */
object Spark06_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    //连接环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)
    //TODO groupBy
    val rdd: RDD[String] = sc.makeRDD(List("Hadoop","Scala","Hello","Spark"), 2)
    //分组和分区没有必然关系
    val groupByRdd: RDD[(Char, Iterable[String])] = rdd.groupBy(s => s.charAt(0))
    groupByRdd.collect().foreach(println)
    //关闭环境
    sc.stop()
  }
}
