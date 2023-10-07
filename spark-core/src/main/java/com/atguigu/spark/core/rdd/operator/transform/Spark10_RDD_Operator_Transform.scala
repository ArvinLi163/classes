package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/09/16 9:25
 * */
object Spark10_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //连接环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)
    //TODO
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)
    //默认是不重新分区的，会导致数据倾斜
    //val newRdd: RDD[Int] = rdd.coalesce(2)
    //第二个参数设置为true，进行shuffle解决数据倾斜,数据没有规律性
    val newRdd: RDD[Int] = rdd.coalesce(2, true)

    newRdd.saveAsTextFile("output")

    //关闭环境
    sc.stop()
  }
}
