package com.atguigu.sparl.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/10/14 16:01
 * */
object Spark02_SparkSQL_UDF {
  def main(args: Array[String]): Unit = {
    //创建环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")
    //自定义函数
    spark.udf.register("prefixName", (username: String) => {
      "Username: " + username
    })
    spark.sql("select age,prefixName(username) from user").show()

    //关闭环境
    spark.stop()
  }
}
