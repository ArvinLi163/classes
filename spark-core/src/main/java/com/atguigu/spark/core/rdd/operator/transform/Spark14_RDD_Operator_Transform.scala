package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/16 9:25
 * */
object Spark14_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //连接环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)
    //TODO key-value类型
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val keyRdd: RDD[(Int, Int)] = rdd.map(num => (num, 1))
    //partitionBy 将数据重新指定分区
    keyRdd.partitionBy(new HashPartitioner(2)).saveAsTextFile("output")
    //关闭环境
    sc.stop()
  }
}
