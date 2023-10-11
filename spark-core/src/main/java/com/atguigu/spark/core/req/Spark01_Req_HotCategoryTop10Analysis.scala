package com.atguigu.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: bansheng
 * @date: 2023/10/11 11:19
 * */
object Spark01_Req_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    //TODO Top10热门品类
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparkConf)
    //1.数据读取
    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")
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
    //点击数量排序，下单数量排序，支付数量排序
    //元组排序，先比较第一个，再比较第二个，再比较第三个，以此类推
    //（品类ID，（点击数量，下单数量，支付数量））
    //cofroup=connect+group
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickCountRDD.cogroup(orderCountRDD, payCountRDD)
    val analysisRDD = cogroupRDD.mapValues {
      case (clickIter, orderIter, payIter) => {
        var clickCnt = 0
        val iter1 = clickIter.iterator
        if (iter1.hasNext) {
          clickCnt = iter1.next()
        }

        var orderCnt = 0
        val iter2 = orderIter.iterator
        if (iter2.hasNext) {
          orderCnt = iter2.next()
        }

        var payCnt = 0
        val iter3 = payIter.iterator
        if (iter3.hasNext) {
          payCnt = iter3.next()
        }
        (clickCnt, orderCnt, payCnt)
      }
    }
    val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)
    //6.将结果采集到控制台打印出来
    resultRDD.foreach(println)
  }
}
