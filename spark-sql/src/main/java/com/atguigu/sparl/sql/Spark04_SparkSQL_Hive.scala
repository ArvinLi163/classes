package com.atguigu.sparl.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/10/14 16:01
 * */
object Spark04_SparkSQL_Hive {
  def main(args: Array[String]): Unit = {
    //创建环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    print(spark)

    //关闭环境
    spark.stop()
  }
}
