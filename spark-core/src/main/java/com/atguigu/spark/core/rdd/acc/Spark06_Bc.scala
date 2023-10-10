package com.atguigu.spark.core.rdd.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @description:
 * @author: bansheng
 * @date: 2023/10/10 15:43
 * */
object Spark06_Bc {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Acc")
    val sc = new SparkContext(sparkConf)
    val rdd1: RDD[(String, Int)] = sc.makeRDD(
      List(("a", 1), ("b", 2), ("c", 3)))
    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))
   //封装广播变量
   val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)
    rdd1.map {
      case (w, c) => {
        val i: Int = bc.value.getOrElse(w, 0)
        (w, (c, i))
      }
    }.collect().foreach(println)

    sc.stop()
  }
}
