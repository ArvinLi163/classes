package com.atguigu.sparl.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/10/14 16:01
 * */
object Spark03_SparkSQL_UDAF {
  def main(args: Array[String]): Unit = {
    //创建环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
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
  /*
  自定义聚合函数类：求年龄的平均值
  1.继承UserDefinedAggregateFunction
  2.重写方法
   */
  class MyAvgUDAF extends UserDefinedAggregateFunction{
    override def inputSchema: StructType = {
      StructType(
        Array(StructField("age",LongType))
      )
    }

    override def bufferSchema: StructType = ???

    override def dataType: DataType = ???

    override def deterministic: Boolean = ???

    override def initialize(buffer: MutableAggregationBuffer): Unit = ???

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = ???

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???

    override def evaluate(buffer: Row): Any = ???
  }
}
