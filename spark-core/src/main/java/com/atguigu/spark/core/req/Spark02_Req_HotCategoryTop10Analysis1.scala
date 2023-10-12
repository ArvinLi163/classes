package com.atguigu.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/10/11 11:19
 * */
object Spark02_Req_HotCategoryTop10Analysis1 {
  def main(args: Array[String]): Unit = {
    //TODO Top10热门品类
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparkConf)
    //1.数据读取
    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    //持久化在内存中
    actionRDD.cache()


    //2.统计商品ID、点击量 (品类ID，点击数量)
    val clickActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        //品类ID=-1,表示该数据不是点击数据
        datas(6) != "-1"
      }
    )
    val clickCountRDD: RDD[(String, Int)] = clickActionRDD.map(action => {
      val datas: Array[String] = action.split("_")
      (datas(6), 1)
    }).reduceByKey(_ + _)
    //3.统计品类的下单数量 (品类ID，下单数量)
    val orderActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        //不是下单行为则为null
        datas(8) != "null"
      }
    )
    //1,2,3=>[(1,1),(2,1),(3,1)]
    val orderCountRDD = orderActionRDD.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        val cid = datas(8)
        val cids: Array[String] = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)
    //4.统计品类的支付数量 (品类ID，支付数量)
    val payActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        //不是支付行为则为null
        datas(10) != "null"
      }
    )
    //1,2,3=>[(1,1),(2,1),(3,1)]
    val payCountRDD = payActionRDD.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        val pid = datas(8)
        val pids: Array[String] = pid.split(",")
        pids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)
    //5.将品类进行排序，并且取前10名
    //（品类ID，点击数量）=>（品类ID，(点击数量,0,0)）,（品类ID，下单数量）=>（品类ID，(0,下单数量，0)），
    // （品类ID，支付数量）=>品类ID，(0,0,支付数量)）
    //（品类ID，（点击数量，下单数量，支付数量））
    val clickRDD: RDD[(String, (Int, Int, Int))] = clickCountRDD.map {
      case (cid, cnt) => (cid, (cnt, 0, 0))
    }
    val orderRDD: RDD[(String, (Int, Int, Int))] = orderCountRDD.map {
      case (oid, cnt) => (oid, (0, cnt, 0))
    }
    val payRDD: RDD[(String, (Int, Int, Int))] = payCountRDD.map {
      case (pid, cnt) => (pid, (0, 0, cnt))
    }
    //将三个数据源，统一进行聚合
    val sourceRDD: RDD[(String, (Int, Int, Int))] = clickRDD.union(orderRDD).union(payRDD)
    val analysisRDD: RDD[(String, (Int, Int, Int))] = sourceRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)
    //6.将结果采集到控制台打印出来
    resultRDD.foreach(println)
    sc.stop()
  }
}
