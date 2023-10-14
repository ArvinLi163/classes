package com.atguigu.sparl.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/10/14 16:01
 * */
object Spark01_SparkSQL_Basic {
  def main(args: Array[String]): Unit = {
    //创建环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    //DataFrame
    //val df: DataFrame = spark.read.json("datas/user.json")
    //df.show()

    //DataFrame =>SQL
    //    df.createOrReplaceTempView("user")
    //    spark.sql("select username from user").show()
    //    spark.sql("select avg(age) from user").show()

    //DataFrame => DSL

    //    df.select("username").show
    //    df.filter($"age" > 22).show
    //    df.select('age+1 as "newage").show()

    //DataSet
    //    val seq: Seq[Int] = Seq(1, 2, 3, 4)
    //    val ds: Dataset[Int] = seq.toDS()
    //    ds.show()

    //RDD <=> DataFrame
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40)))
    val df: DataFrame = rdd.toDF()
    val rdd1: RDD[Row] = df.rdd

    //DataFrame <=> DataSet
    val ds: Dataset[User] = df.as[User]
    val df1: DataFrame = ds.toDF()

    //RDD <=> DataSet
    val ds1: Dataset[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()
    val rdd2: RDD[User] = ds1.rdd
    //关闭环境
    spark.stop()
  }

  case class User(id: Int, name: String, age: Int)
}
