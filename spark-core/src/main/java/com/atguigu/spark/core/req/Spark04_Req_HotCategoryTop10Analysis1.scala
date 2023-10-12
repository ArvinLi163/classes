package com.atguigu.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @description:
 * @author: bansheng
 * @date: 2023/10/11 11:19
 * */
object Spark04_Req_HotCategoryTop10Analysis1 {
  def main(args: Array[String]): Unit = {
    //TODO Top10热门品类
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparkConf)
    //1.数据读取
    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    //创建累加器对象
    val acc = new HotCategoryAccumulator()
    //向spark注册
    sc.register(acc, "hotCategory")
    //2.转换数据结构
    actionRDD.foreach(
      action => {
        val datas: Array[String] = action.split("_")
        if (datas(6) != "-1") {
          //点击
          acc.add(datas(6), "click")
        } else if (datas(8) != "null") {
          //下单
          val oid: Array[String] = datas(8).split(",")
          oid.foreach(
            id => {
              acc.add(id, "order")
            })
        } else if (datas(10) != "null") {
          //支付
          val pid: Array[String] = datas(10).split(",")
          pid.foreach(
            id => {
              acc.add(id, "pay")
            })
        }
      }
    )
    val accRDD: mutable.Map[String, HotCategory] = acc.value
    val categories: mutable.Iterable[HotCategory] = accRDD.map(_._2)
    //排序
    val result: List[HotCategory] = categories.toList.sortWith(
      (left: HotCategory, right: HotCategory) => {
        if (left.clickCnt > right.clickCnt) true
        else if (left.clickCnt == right.clickCnt) {
          if (left.orderCnt > right.orderCnt) true
          else if (left.orderCnt == right.orderCnt) left.payCnt > right.payCnt
          else false
        }
        else false
      }
    )
    //取前1o并打印在控制台上
    result.take(10).foreach(println)
    sc.stop()
  }

  //样例类
  case class HotCategory(cid: String, var clickCnt: Int, var orderCnt: Int, var payCnt: Int)

  /**
   * 自定义累加器
   * 1.继承AccumulatorV2
   * IN：（品类ID，行为类型）
   * OUT: mutable.Map[String,HotCategory]
   */
  class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {
    private val hcMap = mutable.Map[String, HotCategory]()

    override def isZero: Boolean = {
      hcMap.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
      new HotCategoryAccumulator
    }

    override def reset(): Unit = {
      hcMap.clear()
    }

    override def add(v: (String, String)): Unit = {
      //品类ID
      val cid: String = v._1
      //行为类型
      val actionType: String = v._2
      val category: HotCategory = hcMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
      if (actionType == "click") {
        category.clickCnt += 1
      } else if (actionType == "order") {
        category.orderCnt += 1
      } else if (actionType == "pay") {
        category.payCnt += 1
      }
      //将更新的值进行返回
      hcMap.update(cid, category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val map1 = this.hcMap
      val map2: mutable.Map[String, HotCategory] = other.value
      map2.foreach {
        case (cid, hc) => {
          val category: HotCategory = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
          category.clickCnt += hc.clickCnt
          category.orderCnt += hc.orderCnt
          category.payCnt += hc.payCnt
          map1.update(cid, category)
        }
      }
    }

    override def value: mutable.Map[String, HotCategory] = {
      hcMap
    }
  }
}
