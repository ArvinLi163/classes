package com.atguigu.spark.core.rdd.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
 * @description:自定义分区器
 * @author: bansheng
 * @date: 2023/10/10 15:04
 * */
object Spark01_RDD_Partition {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, String)] = sc.makeRDD(List(
      ("nba", "xxxxxxxxxxxxxx"),
      ("wnba", "xxxxxxxxxxxxxx"),
      ("cba", "xxxxxxxxxxxxxx"),
      ("nba", "xxxxxxxxxxxxxx")
    ), 3)
    val partitionRdd: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)
    partitionRdd.saveAsTextFile("output")
    sc.stop()
  }

  /**
   * 自定义分区器
   * 1.继承Partitioner
   * 2.重写方法
   */
  class MyPartitioner extends Partitioner {
    //分区数量
    override def numPartitions: Int = 3

    //根据key返回数据的分区索引（从0开始）
    override def getPartition(key: Any): Int = {
      //模式匹配
      key match {
        case "nba" => 0
        case "wnba" => 1
        case _ => 2
      }
    }
  }
}
