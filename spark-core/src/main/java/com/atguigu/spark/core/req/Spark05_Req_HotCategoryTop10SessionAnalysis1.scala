package com.atguigu.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * Top10 热门品类中每个品类的 Top10 活跃 Session 统计
 * 在需求一的基础上，增加每个品类用户 session 的点击统计
 * @author: bansheng
 * @date: 2023/10/11 11:19
 * */
object Spark05_Req_HotCategoryTop10SessionAnalysis1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparkConf)
    //数据读取
    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    actionRDD.cache()
    //top10的品类ID
    val top10Ids: Array[String] = top10Category(actionRDD)
    //1. 过滤原始数据,保留点击和前10品类ID
    val filterActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        //是否是点击
        if (datas(6) != "-1")
        //是否是top10品类ID
          top10Ids.contains(datas(6))
        else false
      }
    )
    //2.根据品类ID和sessionid进行点击量的统计 =>((品类ID，Session ID)，sum)
    val reduceRDD: RDD[((String, String), Int)] = filterActionRDD.map(
      action => {
        val datas: Array[String] = action.split("_")
        ((datas(6), datas(2)), 1)
      }
    ).reduceByKey(_ + _)
    //3.将统计的结果进行结构的转换
    val mapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case ((cid, sid), sum) => {
        (cid, (sid, sum))
      }
    }
    //4.将相同品类的数据进行分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()
    //5.将分组后的数据进行排序，取出前10
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
    )
    //采集结果打印在控制台上
    resultRDD.collect().foreach(println)
    sc.stop()
  }

  def top10Category(actionRDD: RDD[String]): Array[String] = {
    //1.转换数据结构
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
    //2.将相同品类ID的数据进行分组聚合 =>（品类ID，（点击数量，下单数量，支付数量））
    val analysisRDD: RDD[(String, (Int, Int, Int))] = flatRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      })

    //3.将统计结果根据数量进行降序处理，取前10名的品类ID
    analysisRDD.sortBy(_._2, false).take(10).map(_._1)
  }
}
