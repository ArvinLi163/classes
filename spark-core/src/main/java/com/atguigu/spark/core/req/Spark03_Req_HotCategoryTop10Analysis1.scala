package com.atguigu.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/10/11 11:19
 * */
object Spark03_Req_HotCategoryTop10Analysis1 {
  def main(args: Array[String]): Unit = {
    //TODO Top10热门品类
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparkConf)
    //1.数据读取
    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")
   //存在大量的shuffle操作（reduceByKey）
    // reduceByKey聚合算子，spark会提供优化，缓存

    //2.转换数据结构
    //点击：(品类ID,(1,0,0))
    //下单：(品类ID,(0,1,0))
    //支付：(品类ID,(0,0,1))
    val flatRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        if (datas(6) != "-1") {
          //点击
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          //下单
          val oid: Array[String] = datas(8).split(",")
          oid.map(id => (id, (0, 1, 0)))
        } else if (datas(10) != "null") {
          //支付
          val pid: Array[String] = datas(10).split(",")
          pid.map(id => (id, (0, 1, 0)))
        } else {
          Nil
        }
      }
    )
    //3.将相同品类ID的数据进行分组聚合 =>（品类ID，（点击数量，下单数量，支付数量））
    val analysisRDD: RDD[(String, (Int, Int, Int))] = flatRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      })

    //4.将统计结果根据数量进行降序处理，取前10名
    val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)
    resultRDD.foreach(println)
    sc.stop()
  }
}
