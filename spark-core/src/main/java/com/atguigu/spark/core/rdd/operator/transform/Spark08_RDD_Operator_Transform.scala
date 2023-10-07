package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/16 9:25
 * */
object Spark08_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //连接环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)
    //TODO sample
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    //第一个参数表示每个数据被取出后是否放回，false表示不放回（即丢弃）,true表示放回
    //第二个参数，如果不放回，表示每个数据被取出的概率
    //放回表示，数据被取出可能出现的次数
    //第三个参数表示随机种子，如果没有随机种子，则会取系统默认时间
    println(rdd.sample(true,
//      0.4
      //1
      2).collect().mkString(","))


    //关闭环境
    sc.stop()
  }
}
